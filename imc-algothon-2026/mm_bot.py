"""
- tick_size=1 (integer prices)
- No amend_order: cancel + re-place
- 1 req/s rate limit: SSE-driven, throttled execution
- fps.json fair value anchor blended with microprice
"""

import json
import math
import time
import threading
from datetime import datetime
from dataclasses import dataclass

import numpy as np

from bot_template import BaseBot, Side, OrderRequest, OrderBook, Order, Trade

def microprice_calculation(best_bid, best_bid_volume, best_ask, best_ask_volume, method="microprice"):
    """Calculate microprice or midprice."""
    if best_bid_volume <= 0 or best_ask_volume <= 0:
        return (best_ask + best_bid) / 2
    if method == "midprice":
        return (best_ask + best_bid) / 2
    return (best_bid * best_ask_volume + best_ask * best_bid_volume) / (best_bid_volume + best_ask_volume)


def orderbook_imbalance_calculation(best_bid_volume, best_ask_volume, tick_size):
    """Calculate orderbook imbalance with tanh scaling."""
    if best_bid_volume <= 0 or best_ask_volume <= 0:
        return 0
    unscaled_imbalance = (best_bid_volume - best_ask_volume) / (best_bid_volume + best_ask_volume)
    k = 5
    orderbook_imbalance = np.tanh(k * (unscaled_imbalance ** 3))
    return orderbook_imbalance * tick_size


def inventory_risk_calculation(current_position, position_limit, tick_size):
    """Calculate inventory risk with smooth boost."""
    if position_limit <= 0:
        return 0
    abs_inventory = min(abs(current_position), position_limit)
    scaling_factor = abs_inventory / position_limit

    def smooth_boost(x):
        denom = x ** 2 + (1 - x) ** 2
        return x ** 2 / denom if denom > 0 else 0

    scaling_factor = smooth_boost(scaling_factor)
    return scaling_factor * tick_size


def volatility_calculation(history_microprices, tick_size, window_size=20):
    """Calculate realized volatility in ticks."""
    if tick_size <= 0 or len(history_microprices) < window_size + 1:
        return 0.0
    segment = np.asarray(history_microprices[-(window_size + 1):], dtype=float)
    segment = segment[segment > 0]
    if len(segment) < 2:
        return 0.0
    returns = np.diff(np.log(segment))
    sigma = float(np.sqrt(np.mean(returns * returns)))
    last_price = float(segment[-1])
    return sigma * (last_price / tick_size)


def bound_impact(value, min_value, max_value):
    """Clamp value to [min_value, max_value]."""
    return max(min_value, min(max_value, value))


def pricing_calculation(best_bid, best_bid_volume, best_ask, best_ask_volume,
                        history_microprices, current_position, fair_anchor, params):
    """
    Calculate bid and ask prices.
    
    Blends microprice with external fair_anchor when available.
    fair_anchor: external fair value from fps.json (or None to use pure microprice).
    """
    tick_size = params["tick_size"]
    microprice = microprice_calculation(
        best_bid, best_bid_volume, best_ask, best_ask_volume,
        method=params["fair_value_method"]
    )

    # Blend microprice with external fair value anchor
    if fair_anchor is not None:
        alpha = params.get("fair_anchor_weight", 0.5)
        fair_value = alpha * fair_anchor + (1 - alpha) * microprice
    else:
        fair_value = microprice

    inventory_risk = inventory_risk_calculation(
        current_position, params["position_limit"], tick_size
    ) * params["inventory_risk"]
    obi = orderbook_imbalance_calculation(
        best_bid_volume, best_ask_volume, tick_size
    ) * params["orderbook_imbalance"]
    vol = volatility_calculation(
        history_microprices, tick_size, params["window_size"]
    ) * params["volatility"]
    base_spread = params["base_spread"]

    # Half spread
    base_spread = bound_impact(base_spread, 0, 3 * tick_size)
    vol = bound_impact(vol, 0, 3 * tick_size)
    half_spread = base_spread + vol
    half_spread = bound_impact(half_spread, tick_size, 5 * tick_size)

    # Skew
    obi = bound_impact(obi, -2 * tick_size, 2 * tick_size)
    inventory_risk = bound_impact(inventory_risk, 0, 3 * tick_size)

    if current_position > 0:
        inventory_risk = -inventory_risk  # skew towards selling
    elif current_position < 0:
        inventory_risk = inventory_risk   # skew towards buying
    else:
        inventory_risk = 0

    skew = inventory_risk + obi
    skew = bound_impact(skew, -3 * tick_size, 3 * tick_size)

    ask_price = fair_value + half_spread + skew
    bid_price = fair_value - half_spread + skew

    return bid_price, ask_price


def position_sizing_calculation(current_position, position_limit, max_post_volume,
                                side="bid", outstanding_position=0):
    """Calculate position size respecting limits."""
    outstanding_position = max(0, outstanding_position)

    if side == "bid":
        capacity = max(0, position_limit - current_position - outstanding_position)
    else:
        capacity = max(0, position_limit + current_position - outstanding_position)

    volume = max(0, min(max_post_volume, capacity))

    # Safety check
    if side == "bid":
        if current_position + outstanding_position + volume > position_limit:
            return 0
    else:
        if current_position - outstanding_position - volume < -position_limit:
            return 0

    return volume


def execution_logic(side, model_price, best_bid, best_ask, tick_size,
                    our_price=None, join_if_within_ticks=0.5,
                    max_improve_ticks=1, max_stepback_ticks=1, spread_gate_in_ticks=1):
    """
    Decide: join / improve / stepback, and whether to cancel existing order.
    Returns (price, cancel_action, place_action).
    """
    if model_price is None:
        touch = best_bid if side == "bid" else best_ask
        return touch, "nothing", "join"

    rounded_price = round(model_price / tick_size) * tick_size
    if side == "bid":
        legal_price = min(rounded_price, best_ask - tick_size)
        touch_price = best_bid
    else:
        legal_price = max(rounded_price, best_bid + tick_size)
        touch_price = best_ask

    spread = best_ask - best_bid
    spread_ticks = max(spread / tick_size, 1)

    if side == "bid":
        delta_ticks = (legal_price - touch_price) / tick_size
    else:
        delta_ticks = (touch_price - legal_price) / tick_size

    abs_delta_ticks = abs(delta_ticks)

    if delta_ticks >= join_if_within_ticks and spread_ticks >= spread_gate_in_ticks:
        improve_ticks = min(max_improve_ticks, int(round(delta_ticks)))
        if side == "bid":
            price = best_bid + improve_ticks * tick_size
            price = min(price, best_ask - tick_size)
        else:
            price = best_ask - improve_ticks * tick_size
            price = max(price, best_bid + tick_size)
        price = round(price / tick_size) * tick_size
        action2 = "improve"

    elif abs_delta_ticks < join_if_within_ticks:
        price = touch_price
        action2 = "join"

    elif delta_ticks < -join_if_within_ticks:
        stepback_ticks = min(max_stepback_ticks, int(round(-delta_ticks)))
        if side == "bid":
            price = best_bid - stepback_ticks * tick_size
            price = min(price, best_ask - tick_size)
        else:
            price = best_ask + stepback_ticks * tick_size
            price = max(price, best_bid + tick_size)
        price = round(price / tick_size) * tick_size
        action2 = "stepback"

    else:
        price = touch_price
        action2 = "nothing"

    # Cancel decision
    if our_price is None or our_price == 0:
        action1 = "nothing"
    elif abs(price - our_price) >= tick_size / 2:
        action1 = "cancel"
    else:
        action1 = "nothing"

    return price, action1, action2


def run_mm_logic(best_bid, best_bid_volume, best_ask, best_ask_volume,
                 history_microprices, current_position,
                 our_bid, our_ask, our_bid_volume, our_ask_volume,
                 fair_anchor, params):
    """
    Full MM decision: returns (bid_price, bid_vol, ask_price, ask_vol,
                               bid_cancel, ask_cancel, bid_action, ask_action).
    """
    # Exclude our orders from TOB volumes
    adj_bid_vol = best_bid_volume
    adj_ask_vol = best_ask_volume
    if our_bid and our_bid == best_bid:
        adj_bid_vol = max(0, best_bid_volume - our_bid_volume)
    if our_ask and our_ask == best_ask:
        adj_ask_vol = max(0, best_ask_volume - our_ask_volume)

    tick_size = params["tick_size"]
    microprice = microprice_calculation(
        best_bid, adj_bid_vol, best_ask, adj_ask_vol,
        method=params["fair_value_method"]
    )
    history_microprices.append(microprice)
    if len(history_microprices) > 100:
        history_microprices.pop(0)

    bid_price, ask_price = pricing_calculation(
        best_bid, adj_bid_vol, best_ask, adj_ask_vol,
        history_microprices, current_position, fair_anchor, params
    )

    bid_price, bid_cancel, bid_action = execution_logic(
        side="bid", model_price=bid_price,
        best_bid=best_bid, best_ask=best_ask, tick_size=tick_size,
        our_price=our_bid
    )

    ask_price, ask_cancel, ask_action = execution_logic(
        side="ask", model_price=ask_price,
        best_bid=best_bid, best_ask=best_ask, tick_size=tick_size,
        our_price=our_ask
    )

    bid_volume = position_sizing_calculation(
        current_position, params["position_limit"], params["max_post_volume"],
        side="bid", outstanding_position=our_bid_volume
    )
    ask_volume = position_sizing_calculation(
        current_position, params["position_limit"], params["max_post_volume"],
        side="ask", outstanding_position=our_ask_volume
    )

    return bid_price, bid_volume, ask_price, ask_volume, bid_cancel, ask_cancel, bid_action, ask_action


# ============================================================================
# PER-PRODUCT STATE
# ============================================================================

@dataclass
class ProductState:
    """Track per-product MM state."""
    symbol: str
    history_microprices: list
    our_bid_price: float
    our_bid_volume: int
    our_bid_id: str | None
    our_ask_price: float
    our_ask_volume: int
    our_ask_id: str | None
    last_quote_time: float

    @classmethod
    def create(cls, symbol: str):
        return cls(
            symbol=symbol,
            history_microprices=[],
            our_bid_price=0,
            our_bid_volume=0,
            our_bid_id=None,
            our_ask_price=0,
            our_ask_volume=0,
            our_ask_id=None,
            last_quote_time=0,
        )


# ============================================================================
# CMI MARKET MAKING PARAMS (tuned for tick_size=1, integer prices)
# ============================================================================

CMI_MM_PARAMS = {
    "tick_size": 1,
    "volatility": 0.3,
    "orderbook_imbalance": 1.0,
    "inventory_risk": 1.5,
    "base_spread": 2,              # base half-spread in ticks
    "max_post_volume": 20,         # conservative to start
    "position_limit": 100,
    "window_size": 20,
    "fair_value_method": "microprice",
    "fair_anchor_weight": 0.6,     # 60% fps.json, 40% microprice
}

# Per-product param overrides (optional)
PRODUCT_PARAMS = {
    "TIDE_SPOT":  {"base_spread": 3, "max_post_volume": 15},
    "TIDE_SWING": {"base_spread": 4, "max_post_volume": 15},
    "WX_SPOT":    {"base_spread": 3, "max_post_volume": 15},
    "WX_SUM":     {"base_spread": 3, "max_post_volume": 15},
    "LHR_COUNT":  {"base_spread": 3, "max_post_volume": 15},
    "LHR_INDEX":  {"base_spread": 3, "max_post_volume": 15},
    "LON_ETF":    {"base_spread": 4, "max_post_volume": 10},
    "LON_FLY":    {"base_spread": 5, "max_post_volume": 10},
    # Also support numbered symbols
    "1_Tide":     {"base_spread": 3, "max_post_volume": 15},
    "2_Tide":     {"base_spread": 4, "max_post_volume": 15},
    "3_Weather":  {"base_spread": 3, "max_post_volume": 15},
    "4_Weather":  {"base_spread": 3, "max_post_volume": 15},
    "5_Flights":  {"base_spread": 3, "max_post_volume": 15},
    "6_Airport":  {"base_spread": 3, "max_post_volume": 15},
    "7_ETF":      {"base_spread": 4, "max_post_volume": 10},
    "8_Option":   {"base_spread": 5, "max_post_volume": 10},
}

SYMBOL_ALIASES = {
    "1_Tide": "TIDE_SPOT", "TIDE_SPOT": "1_Tide",
    "2_Tide": "TIDE_SWING", "TIDE_SWING": "2_Tide",
    "3_Weather": "WX_SPOT", "WX_SPOT": "3_Weather",
    "4_Weather": "WX_SUM", "WX_SUM": "4_Weather",
    "5_Flights": "LHR_COUNT", "LHR_COUNT": "5_Flights",
    "6_Airport": "LHR_INDEX", "LHR_INDEX": "6_Airport",
    "7_ETF": "LON_ETF", "LON_ETF": "7_ETF",
    "8_Option": "LON_FLY", "LON_FLY": "8_Option",
}


# ============================================================================
# MM BOT
# ============================================================================

class MMBot(BaseBot):
    """
    Market Making Bot for CMI Exchange.
    
    Uses Optiver-style MM algorithm:
    - microprice + OBI + inventory risk + volatility → bid/ask
    - join/improve/stepback execution logic
    - fps.json fair value as anchor
    - SSE orderbook events drive the loop
    """

    def __init__(self, cmi_url: str, username: str, password: str,
                 params: dict | None = None, quote_interval: float = 5.0,
                 poll_interval: float = 30.0):
        super().__init__(cmi_url, username, password)
        self.base_params = {**CMI_MM_PARAMS, **(params or {})}
        self.quote_interval = quote_interval  # min seconds between requotes per product
        self.poll_interval = poll_interval    # seconds between full product polling rounds
        self.product_states: dict[str, ProductState] = {}
        self.fps: dict[str, float] = {}
        self.fps_lock = threading.Lock()
        self._fps_last_load: float = 0
        self._positions: dict[str, int] = {}
        self._positions_lock = threading.Lock()
        self._positions_last_fetch: float = 0
        self._orders_cache: list[dict] = []
        self._request_lock = threading.Lock()  # serialize API calls for rate limit
        self._last_request_time: float = 0
        self._poll_thread: threading.Thread | None = None
        self._poll_running = False
        self._known_products: list[str] = []

    def log(self, msg: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}][MM] {msg}")

    # -- Rate-limited request wrapper --

    def _throttle(self):
        """Ensure at least 1 second between API requests."""
        with self._request_lock:
            now = time.time()
            elapsed = now - self._last_request_time
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            self._last_request_time = time.time()

    # -- Data loading --

    def _load_fps(self):
        """Load fair prices from fps.json (cached for 10s)."""
        now = time.time()
        if now - self._fps_last_load < 10:
            return
        try:
            with open("fps.json", "r", encoding="utf-8") as f:
                data = json.load(f)
            with self.fps_lock:
                self.fps = {k: float(v) for k, v in data.items()}
            self._fps_last_load = now
        except Exception:
            pass

    def _get_fair_anchor(self, product: str) -> float | None:
        """Get external fair value for a product from fps.json."""
        with self.fps_lock:
            if product in self.fps:
                return self.fps[product]
            alias = SYMBOL_ALIASES.get(product)
            if alias and alias in self.fps:
                return self.fps[alias]
        return None

    def _get_params(self, product: str) -> dict:
        """Get MM params with per-product overrides."""
        params = dict(self.base_params)
        overrides = PRODUCT_PARAMS.get(product, {})
        params.update(overrides)
        return params

    def _get_product_state(self, product: str) -> ProductState:
        """Get or create per-product state."""
        if product not in self.product_states:
            self.product_states[product] = ProductState.create(product)
        return self.product_states[product]

    def _refresh_positions(self):
        """Fetch positions (throttled, cached for 3s)."""
        now = time.time()
        if now - self._positions_last_fetch < 3:
            return
        try:
            self._throttle()
            positions = self.get_positions()
            with self._positions_lock:
                self._positions = positions
            self._positions_last_fetch = now
        except Exception as e:
            self.log(f"Position fetch error: {e}")

    def _refresh_orders(self, product: str) -> tuple[float, int, str | None, float, int, str | None]:
        """
        Parse our outstanding orders for a product.
        Returns (our_bid_price, our_bid_vol, our_bid_id, our_ask_price, our_ask_vol, our_ask_id).
        """
        state = self._get_product_state(product)
        try:
            self._throttle()
            orders = self.get_orders(product=product)
        except Exception:
            orders = []

        bid_price, bid_vol, bid_id = 0, 0, None
        ask_price, ask_vol, ask_id = 0, 0, None

        for o in orders:
            side = str(o.get("side", "")).upper()
            remaining = int(o.get("volume", 0)) - int(o.get("filled", 0))
            if remaining <= 0:
                continue
            if side == "BUY":
                if o.get("price", 0) > bid_price:
                    bid_price = float(o["price"])
                    bid_vol = remaining
                    bid_id = o.get("id")
            elif side == "SELL":
                if ask_price == 0 or o.get("price", 0) < ask_price:
                    ask_price = float(o["price"])
                    ask_vol = remaining
                    ask_id = o.get("id")

        state.our_bid_price = bid_price
        state.our_bid_volume = bid_vol
        state.our_bid_id = bid_id
        state.our_ask_price = ask_price
        state.our_ask_volume = ask_vol
        state.our_ask_id = ask_id

        return bid_price, bid_vol, bid_id, ask_price, ask_vol, ask_id

    # -- Order execution --
    def _cancel_order_safe(self, order_id: str | None):
        """Cancel an order safely."""
        if not order_id:
            return
        try:
            self._throttle()
            self.cancel_order(order_id)
        except Exception as e:
            self.log(f"Cancel error {order_id[:8]}: {e}")

    def _place_order(self, product: str, price: float, volume: int, side: Side) -> str | None:
        """Place an order and return its ID."""
        price = int(round(price))  # CMI requires integer prices
        if price <= 0 or volume <= 0:
            return None
        try:
            self._throttle()
            order = OrderRequest(product=product, price=price, volume=volume, side=side)
            resp = self.send_order(order)
            if resp:
                return resp.id
        except Exception as e:
            self.log(f"Place order error {product} {side} {volume}@{price}: {e}")
        return None

    # -- Core MM execution for one product --

    def _execute_mm(self, orderbook: OrderBook):
        """Run MM logic for a single product triggered by orderbook update."""
        product = orderbook.product
        state = self._get_product_state(product)
        params = self._get_params(product)

        # Throttle: don't requote too often
        now = time.time()
        if now - state.last_quote_time < self.quote_interval:
            return

        # Need valid two-sided book
        if not orderbook.buy_orders or not orderbook.sell_orders:
            return

        best_bid = orderbook.buy_orders[0]
        best_ask = orderbook.sell_orders[0]

        if best_bid.price <= 0 or best_ask.price <= 0 or best_bid.price >= best_ask.price:
            return

        # Load external fair value
        self._load_fps()
        fair_anchor = self._get_fair_anchor(product)

        # Refresh positions
        self._refresh_positions()
        with self._positions_lock:
            current_position = self._positions.get(product, 0)

        # Refresh our outstanding orders for this product
        our_bid_price, our_bid_vol, our_bid_id, our_ask_price, our_ask_vol, our_ask_id = \
            self._refresh_orders(product)

        # Run MM algo
        (mm_bid_price, mm_bid_vol, mm_ask_price, mm_ask_vol,
         bid_cancel, ask_cancel, bid_action, ask_action) = run_mm_logic(
            best_bid=best_bid.price,
            best_bid_volume=best_bid.volume,
            best_ask=best_ask.price,
            best_ask_volume=best_ask.volume,
            history_microprices=state.history_microprices,
            current_position=current_position,
            our_bid=our_bid_price,
            our_ask=our_ask_price,
            our_bid_volume=our_bid_vol,
            our_ask_volume=our_ask_vol,
            fair_anchor=fair_anchor,
            params=params,
        )

        # Round to integer
        mm_bid_price = int(math.floor(mm_bid_price))
        mm_ask_price = int(math.ceil(mm_ask_price))

        # Ensure bid < ask
        if mm_bid_price >= mm_ask_price:
            mid = (mm_bid_price + mm_ask_price) / 2
            mm_bid_price = int(math.floor(mid - 1))
            mm_ask_price = int(math.ceil(mid + 1))

        self.log(
            f"{product}: pos={current_position} fair={fair_anchor} "
            f"bid={mm_bid_price}x{mm_bid_vol}({bid_action}) "
            f"ask={mm_ask_price}x{mm_ask_vol}({ask_action}) "
            f"mkt=[{best_bid.price}/{best_ask.price}]"
        )

        # Execute: Cancel then place (seems no amend)
        need_cancel_bid = (bid_cancel == "cancel" and our_bid_id)
        need_cancel_ask = (ask_cancel == "cancel" and our_ask_id)
        need_place_bid = (bid_action in ["improve", "join", "stepback"] and mm_bid_vol > 0)
        need_place_ask = (ask_action in ["improve", "join", "stepback"] and mm_ask_vol > 0)

        # If price unchanged and we have an order, skip cancel+place (preserve queue priority)
        if our_bid_id and not need_cancel_bid and our_bid_price == mm_bid_price:
            need_place_bid = False
        if our_ask_id and not need_cancel_ask and our_ask_price == mm_ask_price:
            need_place_ask = False

        # Cancel stale orders
        if need_cancel_bid:
            self._cancel_order_safe(our_bid_id)
            state.our_bid_id = None

        if need_cancel_ask:
            self._cancel_order_safe(our_ask_id)
            state.our_ask_id = None

        # Place new orders
        if need_place_bid:
            # Safety: check position limit
            if current_position + mm_bid_vol <= params["position_limit"]:
                new_id = self._place_order(product, mm_bid_price, mm_bid_vol, Side.BUY)
                if new_id:
                    state.our_bid_id = new_id
                    state.our_bid_price = mm_bid_price
                    state.our_bid_volume = mm_bid_vol
            else:
                self.log(f"{product}: BID blocked, pos={current_position}+{mm_bid_vol} > limit")

        if need_place_ask:
            if current_position - mm_ask_vol >= -params["position_limit"]:
                new_id = self._place_order(product, mm_ask_price, mm_ask_vol, Side.SELL)
                if new_id:
                    state.our_ask_id = new_id
                    state.our_ask_price = mm_ask_price
                    state.our_ask_volume = mm_ask_vol
            else:
                self.log(f"{product}: ASK blocked, pos={current_position}-{mm_ask_vol} < -limit")

        state.last_quote_time = time.time()

    # -- Proactive polling for ALL products --

    def _execute_mm_for_product(self, product: str):
        """Fetch orderbook via REST and run MM for a product."""
        try:
            self._throttle()
            ob = self.get_orderbook(product)
            self._execute_mm(ob)
        except Exception as e:
            self.log(f"Poll MM error on {product}: {e}")

    def _poll_loop(self):
        """Background thread: poll all products periodically."""
        # Discover products on first run
        try:
            self._throttle()
            products = self.get_products()
            self._known_products = [p.symbol for p in products]
            self.log(f"Discovered {len(self._known_products)} products: {self._known_products}")
        except Exception as e:
            self.log(f"Failed to discover products: {e}")
            return

        while self._poll_running:
            self.log(f"--- Polling all {len(self._known_products)} products ---")
            for product in self._known_products:
                if not self._poll_running:
                    break
                self._execute_mm_for_product(product)
            # Sleep until next poll round
            for _ in range(int(self.poll_interval)):
                if not self._poll_running:
                    break
                time.sleep(1)

    def start_polling(self):
        """Start the background polling thread."""
        if self._poll_thread and self._poll_thread.is_alive():
            return
        self._poll_running = True
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()
        self.log("Polling thread started")

    def stop_polling(self):
        """Stop the background polling thread."""
        self._poll_running = False
        if self._poll_thread:
            self._poll_thread.join(timeout=5)
            self._poll_thread = None
        self.log("Polling thread stopped")

    # -- BaseBot callbacks --

    def on_orderbook(self, orderbook: OrderBook):
        """SSE orderbook event → run MM for this product."""
        try:
            self._execute_mm(orderbook)
        except Exception as e:
            self.log(f"MM error on {orderbook.product}: {e}")

    def on_trades(self, trade: Trade):
        """Log our fills."""
        side = "BOUGHT" if trade.buyer == self.username else "SOLD"
        self.log(f"FILL: {side} {trade.volume}x {trade.product} @ {trade.price}")


# ============================================================================
# MAIN
# ============================================================================

TEST_EXCHANGE = "http://ec2-52-49-69-152.eu-west-1.compute.amazonaws.com/"
REAL_EXCHANGE = ""  # TODO

USERNAME = "test9"       # TODO: change
PASSWORD = "1233211234567!"  # TODO: change

if __name__ == "__main__":
    bot = MMBot(
        cmi_url=TEST_EXCHANGE,
        username=USERNAME,
        password=PASSWORD,
        quote_interval=5.0,  # seconds between requotes per product
    )

    bot.log("Starting MM Bot...")
    bot.start()           # SSE stream for reactive MM
    bot.start_polling()   # Background polling for ALL products
    bot.log("MM Bot started: SSE + polling active for all products")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        bot.log("Shutting down...")
        bot.stop_polling()
        bot.cancel_all_orders()
        bot.stop()
        bot.log("MM Bot stopped.")
