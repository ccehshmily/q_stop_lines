from collections import OrderedDict
import math

class Holding:
    def __init__(self, security, cash):
        self.security = security
        self.cash = cash
        self.num_stocks = 0
        self.open_buy_order_price = 0
        self.open_buy_order_number = 0
        self.open_sell_order_price = 0
        self.open_sell_order_number = 0

    def order_buy(self, number, price):
        print "holding change: buy " + str(number) + " of " + self.security + " at " + str(price)
        self.cash -= number * price
        self.open_buy_order_price = price
        self.open_buy_order_number = number

    def cancel_open_buy_order_and_update(self, number_canceled):
        print "holding change: cancel buy " + str(number_canceled) + " of " + self.security
        self.cash += number_canceled * self.open_buy_order_price
        self.num_stocks += self.open_buy_order_number - number_canceled
        self.open_buy_order_price = 0
        self.open_buy_order_number = 0

    def order_sell(self, number, price):
        print "holding change: sell " + str(number) + " of " + self.security + " at " + str(price)
        self.open_sell_order_price = price
        self.open_sell_order_number = number

    def cancel_open_sell_order_and_update(self, number_canceled):
        print "holding change: cancel sell " + str(number_canceled) + " of " + self.security
        self.cash += (self.open_sell_order_number - number_canceled) * self.open_sell_order_price
        self.num_stocks -= self.open_sell_order_number - number_canceled
        self.open_sell_order_price = 0
        self.open_sell_order_number = 0

def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    set_commission(commission.PerTrade(cost=0.00))
    set_slippage(slippage.FixedSlippage(spread=0.00))
    set_long_only()

    context.max_portfolio_size = 10000

    context.min_max_window = 10
    context.cool_out_time = 20
    context.max_confidence_proportion = 0.25

    EveryThisManyMinutes = 3
    TradingDayHours = 5
    TradingDayMinutes=int((TradingDayHours * 60) - (EveryThisManyMinutes * 2))
    for minutez in xrange(
        context.cool_out_time * 3,
        TradingDayMinutes,
        EveryThisManyMinutes
    ):
        schedule_function(my_buy_rebalance, date_rules.every_day(), time_rules.market_open(minutes=minutez))
    for minutez in xrange(
        context.cool_out_time * 3 + 2,
        TradingDayMinutes + 2,
        EveryThisManyMinutes
    ):
        schedule_function(my_sell_rebalance, date_rules.every_day(), time_rules.market_open(minutes=minutez))
    schedule_function(clear_positions, date_rules.every_day(), time_rules.market_close(hours=0, minutes=23))

def before_trading_start(context, data):
    """
    Called every day before market open.
    """
    # These are the securities that we are interested in trading each day.
    context.security = symbol('JNUG')

    context.stop_lines_up = {}
    context.stop_lines_down = {}
    context.ordered_up_lines_confidence = []
    context.ordered_down_lines_confidence = []
    context.confidence_bar_up = 0.0
    context.confidence_bar_down = 0.0
    context.cur_minute = 0

    context.cur_holdings = {}

    context.already_stopped = False

def my_buy_rebalance(context, data):
    if context.already_stopped:
        return

    sec = context.security
    if sec not in context.cur_holdings:
        context.cur_holdings[sec] = Holding(sec, context.max_portfolio_size)
    holding = context.cur_holdings[sec]
    cur_price = float(data.current([sec], 'price'))
    buy_price = 1000000
    my_cost = 1000000
    if sec in context.portfolio.positions:
        my_cost = float(context.portfolio.positions[sec].cost_basis)
    if len(context.ordered_down_lines_confidence) > 0:
        for (price, confidence) in context.ordered_down_lines_confidence:
            if confidence >= context.confidence_bar_down and price < cur_price and price < my_cost:
                buy_price = price

    if holding.open_buy_order_price <> 0 and holding.open_buy_order_price <> buy_price:
        cancel_open_buy_orders(sec, holding)
    if buy_price <> 1000000:
        place_buy_order(sec, buy_price, holding)

def my_sell_rebalance(context, data):
    sec = context.security
    if sec not in context.cur_holdings:
        return
    holding = context.cur_holdings[sec]
    cur_price = float(data.current([sec], 'price'))
    sell_price = 0

    if len(context.ordered_up_lines_confidence) > 0:
        for (price, confidence) in context.ordered_up_lines_confidence:
            if confidence >= context.confidence_bar_up and price > cur_price:
                sell_price = price

    if holding.open_sell_order_price <> 0 and holding.open_sell_order_price <> sell_price:
        cancel_open_sell_orders(sec, holding)
    if sell_price <> 0:
        place_sell_order(context, sec, sell_price, holding)

def place_buy_order(sec, price, holding):
    cash_available = holding.cash
    num_to_order = int(cash_available / price)
    if num_to_order <= 0:
        return
    order(sec, num_to_order, style=LimitOrder(price))
    holding.order_buy(num_to_order, price)

def place_sell_order(context, sec, price, holding):
    num_to_sell = context.portfolio.positions[sec].amount - holding.open_sell_order_number
    if num_to_sell <= 0:
        return
    order(sec, 0 - num_to_sell, style=LimitOrder(price))
    holding.order_sell(num_to_sell, price)

def cancel_open_buy_orders(sec, holding):
    oo = get_open_orders()
    amount_canceled = 0
    if sec in oo:
        orders = oo[sec]
        for order in orders:
            if 0 < order.amount: #it is a buy order
                cancel_order(order)
                amount_canceled += order.amount
    holding.cancel_open_buy_order_and_update(amount_canceled)

def cancel_open_sell_orders(sec, holding):
    oos = get_open_orders(sec)
    amount_canceled = 0
    for order in oos:
        if 0 > order.amount: #it is a sell order
            cancel_order(order)
            amount_canceled -= order.amount
    holding.cancel_open_sell_order_and_update(amount_canceled)

def clear_positions(context, data):
    context.already_stopped = True

    oo = get_open_orders()
    if len(oo) == 0:
        return
    for stock, orders in oo.iteritems():
        for order in orders:
            cancel_order(order)

    for sec in context.portfolio.positions:
        order_target(sec, 0)

def handle_data(context, data):
    """
    Called every minute.
    """
    context.cur_minute += 1
    if context.cur_minute < context.min_max_window + context.cool_out_time:
        return

    price_history = data.history(context.security, "price", context.min_max_window, "1m")
    mid_place = int(context.min_max_window/2)
    price_mid = round(price_history[mid_place], 2)
    if price_mid == max(price_history):
        if price_mid not in context.stop_lines_up:
            context.stop_lines_up[price_mid] = [0, 0]
        context.stop_lines_up[price_mid][0] += 1
    elif price_mid == min(price_history):
        if price_mid not in context.stop_lines_down:
            context.stop_lines_down[price_mid] = [0, 0]
        context.stop_lines_down[price_mid][0] += 1
    elif price_mid > min(price_history[:mid_place]) and price_mid < max(price_history[mid_place + 1:]):
        if price_mid in context.stop_lines_up:
            context.stop_lines_up[price_mid][1] += 1
    elif price_mid < max(price_history[:mid_place]) and price_mid > min(price_history[mid_place + 1:]):
        if price_mid in context.stop_lines_down:
            context.stop_lines_down[price_mid][1] += 1

    ordered_up_lines = OrderedDict(sorted(context.stop_lines_up.items(), key=lambda t: t[0]))
    ordered_down_lines = OrderedDict(sorted(context.stop_lines_down.items(), key=lambda t: t[0], reverse=True))

    context.ordered_up_lines_confidence = [(t[0], round((t[1][0] + 1) * math.log(t[1][0] + 1) / (t[1][1] + 1), 2)) for t in ordered_up_lines.items()]
    context.ordered_down_lines_confidence = [(t[0], round((t[1][0] + 1) * math.log(t[1][0] + 1) / (t[1][1] + 1), 2)) for t in ordered_down_lines.items()]

    up_lines_max_confidence = OrderedDict(sorted(context.ordered_up_lines_confidence, key=lambda t: t[1], reverse=True))
    down_lines_max_confidence = OrderedDict(sorted(context.ordered_down_lines_confidence, key=lambda t: t[1], reverse=True))

    if len(up_lines_max_confidence) > 0:
        context.confidence_bar_up = up_lines_max_confidence.items()[int(len(up_lines_max_confidence)*context.max_confidence_proportion)][1]
    else:
        context.confidence_bar_up = 0

    if len(down_lines_max_confidence) > 0:
        context.confidence_bar_down = down_lines_max_confidence.items()[int(len(down_lines_max_confidence)*context.max_confidence_proportion)][1]
    else:
        context.confidence_bar_down = 1000000

    """
    print "=== Up Stop Lines ==="
    for (price, confidence) in ordered_up_lines_confidence:
        if confidence >= confidence_bar_up:
            print (price, confidence)
    print "=== Down Stop Lines ==="
    for (price, confidence) in ordered_down_lines_confidence:
        if confidence >= confidence_bar_down:
            print (price, confidence)
    """
