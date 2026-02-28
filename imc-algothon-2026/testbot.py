from bot_template import BaseBot, Side, OrderRequest, OrderBook, Order
import json
from trade_weather import fair_price
from trade_flight import get_fair_flight_price
import time
from datetime import datetime

TEST_EXCHANGE = "http://ec2-52-49-69-152.eu-west-1.compute.amazonaws.com/"
REAL_EXCHANGE = "" # TODO

username_test = "test9"  # TODO: Change this to your team's username you've created in CMI
password_test = "1233211234567!"  # TODO: Change this to be your team's password you've created in CMI

username_real = ""
password_real = ""

class CustomBot(BaseBot):
    def __init__(self, cmi_url, username, password):
        super().__init__(cmi_url, username, password)
        self.placed = False
        self.LIMIT = 200
        self.last = time.time()-30
        self.seen_products = set()

    def log(self, msg: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] {msg}")
    # Handler for own trades
    def on_trades(self, trades: list[dict]):
        for trade in trades:
            self.log(f"Trade: {trade['product']} {trade['volume']} @ {trade['price']}")
        

    def strategy_weather(self, my_positions, fps):
        self.log("Start Strategy for Weather")
        s = 50
        key1 = self._pick_symbol(fps, "WX_SPOT", "3_Weather")
        key2 = self._pick_symbol(fps, "WX_SUM", "4_Weather")
        if not key1 and not key2:
            self.log("Weather skip: no matching symbols found in fps/orderbook")
        if key1:
            self.order(key1, fps[key1], my_positions.get(key1, 0), s)
        if key2:
            self.order(key2, fps[key2], my_positions.get(key2, 0), s)
        self.log("End Strategy for Weather")
        
    def strategy_flight(self, my_positions, fps):
        self.log("Start Strategy for Flight")
        key1 = self._pick_symbol(fps, "LHR_COUNT", "5_Flights")
        key2 = self._pick_symbol(fps, "LHR_INDEX", "6_Airport")
        if not key1 and not key2:
            self.log("Flight skip: no matching symbols found in fps/orderbook")
        if key1:
            self.order(key1, fps[key1], my_positions.get(key1, 0), 50)
        if key2:
            self.order(key2, fps[key2], my_positions.get(key2, 0), 200)
        self.log("End Strategy for Flight")

    def strategy_tide_and_derived(self, my_positions, fps):
        self.log("Start Strategy for Tide/Derived")
        placed_any = False
        for keys, spread in [
            (("TIDE_SPOT", "1_Tide"), 50),
            (("TIDE_SWING", "2_Tide"), 80),
            (("LON_ETF", "7_ETF"), 60),
            (("LON_FLY", "8_Option"), 100),
        ]:
            key = self._pick_symbol(fps, *keys)
            if key:
                self.order(key, fps[key], my_positions.get(key, 0), spread)
                placed_any = True
        if not placed_any:
            self.log("Tide/Derived skip: no matching symbols found in fps/orderbook")
        self.log("End Strategy for Tide/Derived")

    def _pick_symbol(self, container, *candidates):
        # Prefer symbols observed from live orderbooks on this exchange.
        for c in candidates:
            if c in self.seen_products and c in container:
                return c
        for c in candidates:
            if c in container:
                return c
        return None

    def order(self, product, fair_price, position, s, normal_vol=20):
        self.log(
            f"Order plan {product}: fair={round(fair_price)} pos={position} spread={s} normal_vol={normal_vol}"
        )
        if position > 0:
            # sell @ s + 
            sell_price = fair_price + s * (self.LIMIT - position)/self.LIMIT
            vol = position
            order_sell = OrderRequest(product=product,
                                   price=round(sell_price),
                                   volume=vol,
                                   side=Side.SELL)
            self.log(f"Send reduce SELL {product}: px={round(sell_price)} vol={vol}")
            self.send_order(order_sell)
            # print(f"Send {}")
        if position < 0:
            buy_price = fair_price - s * (self.LIMIT + position)/self.LIMIT
            vol = -position
            order_buy = OrderRequest(product=product,
                                   price=round(buy_price),
                                   volume=vol,
                                   side=Side.BUY)
            self.log(f"Send reduce BUY {product}: px={round(buy_price)} vol={vol}")
            self.send_order(order_buy)
        
        vol = min([200 - position, normal_vol])
        if vol > 0:
            order_buy_normal = OrderRequest(product=product,
                                    price=round(fair_price - s),
                                    volume=vol,
                                    side=Side.BUY)
            self.log(f"Send normal BUY {product}: px={round(fair_price - s)} vol={vol}")
            self.send_order(order_buy_normal)
        vol = min([200 + position, normal_vol])
        if vol > 0:
            order_sell_normal = OrderRequest(product=product,
                                    price=round(fair_price + s),
                                    volume=vol,
                                    side=Side.SELL)
            self.log(f"Send normal SELL {product}: px={round(fair_price + s)} vol={vol}")
            self.send_order(order_sell_normal)

    def on_orderbook(self, orderbook: OrderBook):
        self.seen_products.add(orderbook.product)
        self.log(f"on_orderbook event: product={orderbook.product}")
        # print(orderbook)的
        if time.time()-self.last > 30:
            self.last = time.time()
            self.log(f"Orderbook tick: product={orderbook.product}")
            self.log("Receiving Orderbook, Canceling orders")
            # self.cancel_all_orders()
            self.log("Load Fairprice from fps.json")
            try:
                with open("fps.json", "r", encoding="utf-8") as f:
                    fps = json.load(f)
            except Exception as exc:
                self.log(f"Failed to read fps.json: {exc}")
                return
            self.log(
                "Loaded fair prices: "
                f"{', '.join(sorted(k for k in fps.keys() if k in {'WX_SPOT','WX_SUM','TIDE_SPOT','TIDE_SWING','LHR_COUNT','LHR_INDEX','LON_ETF','LON_FLY','3_Weather','4_Weather','1_Tide','2_Tide','5_Flights','6_Airport','7_ETF','8_Option'}))}"
            )
            my_positions = self.request_positions()
            self.log(f"Positions snapshot: {my_positions}")
            self.strategy_weather(my_positions, fps)
            self.strategy_flight(my_positions, fps)
            self.strategy_tide_and_derived(my_positions, fps)
        else:
            self.log("Skip strategy run: waiting for 30s throttle window")
        
        # print(self.request_positions())
        # do_i_want_to_buy = False
        # if do_i_want_to_buy:
        #     order = OrderRequest(product=orderbook.product,
        #                          price=orderbook.sell_orders[0].price,
        #                          volume=1,
        #                          side=Side.BUY)
        #     do_i_want_to_buy = False
        #     self.send_order(order)

try:
    
    bot = CustomBot(TEST_EXCHANGE, username_test, password_test)
    #bot = CustomBot(REAL_EXCHANGE, username_real, password_real)
    
    # print(bot.request_positions())

    # print(bot.request_all_orders())
    bot.log("Starting bot")
    bot.start()
    bot.log("Bot started")

    while True:
        pass
except KeyboardInterrupt as e:
    bot.stop()
