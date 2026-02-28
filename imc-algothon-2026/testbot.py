from bot_template import BaseBot, Side, OrderRequest, OrderBook, Order
import json
from trade_weather import fair_price
from trade_flight import get_fair_flight_price
import time

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
    # Handler for own trades
    def on_trades(self, trades: list[dict]):
        for trade in trades:
            print(f"{trade['volume']} @ {trade['price']}")
        

    def strategy_weather(self, my_positions, fps):
        print("Start Strategy for Weather")
        s = 50
        key1 = self._pick_symbol(fps, "WX_SPOT", "3_Weather")
        key2 = self._pick_symbol(fps, "WX_SUM", "4_Weather")
        if key1:
            self.order(key1, fps[key1], my_positions.get(key1, 0), s)
        if key2:
            self.order(key2, fps[key2], my_positions.get(key2, 0), s)
        print("End Strategy for Weather")
        
    def strategy_flight(self, my_positions, fps):
        print("Start Strategy for Flight")
        key1 = self._pick_symbol(fps, "LHR_COUNT", "5_Flights")
        key2 = self._pick_symbol(fps, "LHR_INDEX", "6_Airport")
        if key1:
            self.order(key1, fps[key1], my_positions.get(key1, 0), 50)
        if key2:
            self.order(key2, fps[key2], my_positions.get(key2, 0), 200)
        print("End Strategy for Flight")

    def strategy_tide_and_derived(self, my_positions, fps):
        print("Start Strategy for Tide/Derived")
        for keys, spread in [
            (("TIDE_SPOT", "1_Tide"), 50),
            (("TIDE_SWING", "2_Tide"), 80),
            (("LON_ETF", "7_ETF"), 60),
            (("LON_FLY", "8_Option"), 100),
        ]:
            key = self._pick_symbol(fps, *keys)
            if key:
                self.order(key, fps[key], my_positions.get(key, 0), spread)
        print("End Strategy for Tide/Derived")

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
        if position > 0:
            # sell @ s + 
            sell_price = fair_price + s * (self.LIMIT - position)/self.LIMIT
            vol = position
            order_sell = OrderRequest(product=product,
                                   price=round(sell_price),
                                   volume=vol,
                                   side=Side.SELL)
            # print(f"Send {}")
        if position < 0:
            buy_price = fair_price - s * (self.LIMIT + position)/self.LIMIT
            vol = -position
            order_buy = OrderRequest(product=product,
                                   price=round(buy_price),
                                   volume=vol,
                                   side=Side.BUY)
            self.send_order(order_buy)
        
        vol = min([200 - position, normal_vol])
        if vol > 0:
            order_buy_normal = OrderRequest(product=product,
                                    price=round(fair_price - s),
                                    volume=vol,
                                    side=Side.BUY)
            self.send_order(order_buy_normal)
        vol = min([200 + position, normal_vol])
        if vol > 0:
            order_sell_normal = OrderRequest(product=product,
                                    price=round(fair_price + s),
                                    volume=vol,
                                    side=Side.SELL)
            self.send_order(order_sell_normal)

    def on_orderbook(self, orderbook: OrderBook):
        self.seen_products.add(orderbook.product)
        # print(orderbook)的
        if time.time()-self.last > 30:
            self.last = time.time()
            print("Receiving Orderbook, Canceling orders")
            # self.cancel_all_orders()
            print("Load Fairprice")
            with open("fps.json", "r", encoding="utf-8") as f:
                fps = json.load(f)
            my_positions = self.request_positions()
            self.strategy_weather(my_positions, fps)
            self.strategy_flight(my_positions, fps)
            self.strategy_tide_and_derived(my_positions, fps)
        
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
    bot.start()

    while True:
        pass
except KeyboardInterrupt as e:
    bot.stop()
