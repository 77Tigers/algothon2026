"""
Quick market scanner: check MM opportunity for each product.

Metrics:
1. Spread (ticks) — wider = more profit per round trip
2. Fair value edge — |market_mid - fair_value| / spread
3. Book depth — volume at TOB, thin = risky
4. Our position — how much room we have
"""

import json
import sys
from bot_template import BaseBot, OrderBook, Trade

TEST_EXCHANGE = "http://ec2-52-49-69-152.eu-west-1.compute.amazonaws.com/"
USERNAME = "test9"
PASSWORD = "1233211234567!"


class Scanner(BaseBot):
    def on_orderbook(self, ob): pass
    def on_trades(self, t): pass


def main():
    bot = Scanner(TEST_EXCHANGE, USERNAME, PASSWORD)

    # Load fair prices
    fps = {}
    try:
        with open("fps.json", "r") as f:
            fps = json.load(f)
    except Exception:
        print("Warning: fps.json not found")

    # Alias mapping
    aliases = {
        "1_Tide": "TIDE_SPOT", "TIDE_SPOT": "1_Tide",
        "2_Tide": "TIDE_SWING", "TIDE_SWING": "2_Tide",
        "3_Weather": "WX_SPOT", "WX_SPOT": "3_Weather",
        "4_Weather": "WX_SUM", "WX_SUM": "4_Weather",
        "5_Flights": "LHR_COUNT", "LHR_COUNT": "5_Flights",
        "6_Airport": "LHR_INDEX", "LHR_INDEX": "6_Airport",
        "7_ETF": "LON_ETF", "LON_ETF": "7_ETF",
        "8_Option": "LON_FLY", "LON_FLY": "8_Option",
    }

    def get_fair(symbol):
        if symbol in fps:
            return fps[symbol]
        alt = aliases.get(symbol)
        if alt and alt in fps:
            return fps[alt]
        return None

    products = bot.get_products()
    positions = bot.get_positions()

    print(f"\n{'Product':<14} {'Bid':>8} {'Ask':>8} {'Spread':>7} {'Mid':>8} {'Fair':>8} {'Edge':>7} {'BidVol':>7} {'AskVol':>7} {'Pos':>5} {'MM?':<12}")
    print("-" * 110)

    for p in products:
        sym = p.symbol
        try:
            ob = bot.get_orderbook(sym)
        except Exception:
            print(f"{sym:<14} {'-- no orderbook --':>60}")
            continue

        bids = [o for o in ob.buy_orders if o.volume - o.own_volume > 0]
        asks = [o for o in ob.sell_orders if o.volume - o.own_volume > 0]

        if not bids or not asks:
            best_bid = ob.buy_orders[0] if ob.buy_orders else None
            best_ask = ob.sell_orders[0] if ob.sell_orders else None
            bid_str = f"{best_bid.price:.0f}" if best_bid else "---"
            ask_str = f"{best_ask.price:.0f}" if best_ask else "---"
            fair = get_fair(sym)
            fair_str = f"{fair:.0f}" if fair else "---"
            pos = positions.get(sym, 0)
            print(f"{sym:<14} {bid_str:>8} {ask_str:>8} {'---':>7} {'---':>8} {fair_str:>8} {'---':>7} {'---':>7} {'---':>7} {pos:>5} {'SEED fair':<12}")
            continue

        best_bid = bids[0]
        best_ask = asks[0]
        spread = best_ask.price - best_bid.price
        mid = (best_bid.price + best_ask.price) / 2
        fair = get_fair(sym)
        pos = positions.get(sym, 0)

        # Edge: how far is market mid from our fair value, in ticks
        if fair is not None:
            edge = abs(mid - fair)
            edge_str = f"{edge:.1f}"
        else:
            edge = 0
            edge_str = "---"

        # MM opportunity assessment
        signals = []
        if spread >= 3:
            signals.append("wide")
        if spread >= 6:
            signals.append("VERY wide")
        if fair is not None and edge > spread / 2:
            signals.append("edge!")
        if best_bid.volume < 5 and best_ask.volume < 5:
            signals.append("thin")
        if spread < 2:
            signals.append("tight")

        verdict = ", ".join(signals) if signals else "ok"

        print(
            f"{sym:<14} {best_bid.price:>8.0f} {best_ask.price:>8.0f} {spread:>7.0f} "
            f"{mid:>8.1f} {(f'{fair:.0f}' if fair else '---'):>8} {edge_str:>7} "
            f"{best_bid.volume:>7} {best_ask.volume:>7} {pos:>5} {verdict:<12}"
        )

    print()
    print("Legend:")
    print("  wide/VERY wide = spread big enough for MM profit")
    print("  edge!          = our fair value differs from market mid (directional + MM)")
    print("  thin           = low liquidity at TOB, risky to quote")
    print("  tight          = spread too narrow, little MM profit")
    print("  SEED fair      = one-sided or empty book, can seed with fair value quotes")


if __name__ == "__main__":
    main()
