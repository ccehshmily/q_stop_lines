from collections import OrderedDict
import math

""" Class for recording the holding status of a security. """
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
        print "holding change: buy " + str(number) + " of " + str(self.security) + " at " + str(price)
        self.cash -= number * price
        self.open_buy_order_price = price
        self.open_buy_order_number = number

    def cancel_open_buy_order_and_update(self, number_canceled):
        print "holding change: cancel buy " + str(number_canceled) + " of " + str(self.security)
        self.cash += number_canceled * self.open_buy_order_price
        self.num_stocks += self.open_buy_order_number - number_canceled
        self.open_buy_order_price = 0
        self.open_buy_order_number = 0

    def order_sell(self, number, price):
        print "holding change: sell " + str(number) + " of " + str(self.security) + " at " + str(price)
        self.open_sell_order_price = price
        self.open_sell_order_number = number

    def cancel_open_sell_order_and_update(self, number_canceled):
        print "holding change: cancel sell " + str(number_canceled) + " of " + str(self.security)
        self.cash += (self.open_sell_order_number - number_canceled) * self.open_sell_order_price
        self.num_stocks -= self.open_sell_order_number - number_canceled
        self.open_sell_order_price = 0
        self.open_sell_order_number = 0
""" END Class for recording the holding status of a security. """

""" Methods placing orders and altering holding status. """
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
    oos = get_open_orders(sec)
    amount_canceled = 0
    for order in oos:
        if 0 < order.amount: #it is a buy order
            print "cancel open buy order: " + str(order.amount) + " of " + str(sec) + " | filled: " + str(order.filled)
            cancel_order(order)
            amount_canceled += order.amount - order.filled
    holding.cancel_open_buy_order_and_update(amount_canceled)

def cancel_open_sell_orders(sec, holding):
    oos = get_open_orders(sec)
    amount_canceled = 0
    for order in oos:
        if 0 > order.amount: #it is a sell order
            print "cancel open sell order: " + str(order.amount) + " of " + str(sec) + " | filled: " + str(order.filled)
            cancel_order(order)
            amount_canceled -= order.amount - order.filled
    holding.cancel_open_sell_order_and_update(amount_canceled)
""" END Methods placing orders and altering holding status. """

""" Utilities confirming stop lines with a price. """
def getBuyLineBelowPrice(context, sec, price):
    buy_price = None
    if len(context.ordered_down_lines_confidence[sec]) > 0:
        for (price_line, confidence) in context.ordered_down_lines_confidence[sec]:
            if confidence >= context.confidence_bar_down[sec] and price_line < price:
                buy_price = price_line
    return buy_price

def getSellLineAbovePrice(context, sec, price):
    sell_price = None
    if len(context.ordered_up_lines_confidence[sec]) > 0:
        for (price_line, confidence) in context.ordered_up_lines_confidence[sec]:
            if confidence >= context.confidence_bar_up[sec] and price_line > price:
                sell_price = price_line
    return sell_price
""" END Utilities confirming stop lines with a price. """

""" Methods dealing with positions, buying and selling according to data. """
def buy_rebalance(context, data):
    if context.already_stopped:
        return

    for sec in context.security:
        if sec not in context.cur_holdings:
            # TODO: context.max_portfolio_size will need to be changed to its own portion
            # TODO: clear holding when sec is all sold
            context.cur_holdings[sec] = Holding(sec, context.max_portfolio_size)
        holding = context.cur_holdings[sec]
        cur_price = float(data.current([sec], 'price'))
        my_cost = context.MAX_NUMBER
        if sec in context.portfolio.positions:
            my_cost = float(context.portfolio.positions[sec].cost_basis)

        buy_price = getBuyLineBelowPrice(context, sec, min(cur_price, my_cost))

        if holding.open_buy_order_price <> 0 and holding.open_buy_order_price <> buy_price:
            cancel_open_buy_orders(sec, holding)
        if buy_price <> None:
            place_buy_order(sec, buy_price, holding)

def sell_rebalance(context, data):
    if context.already_stopped:
        return

    for sec in context.security:
        if sec not in context.cur_holdings:
            return
        holding = context.cur_holdings[sec]
        cur_price = float(data.current([sec], 'price'))
        my_cost = None
        if sec in context.portfolio.positions:
            my_cost = float(context.portfolio.positions[sec].cost_basis)

        sell_price = getSellLineAbovePrice(context, sec, cur_price)

        if my_cost <> None and cur_price < my_cost:
            possible_buy_price = getBuyLineBelowPrice(context, sec, cur_price)
            if possible_buy_price == None:
                sell_price = cur_price

        if holding.open_sell_order_price <> 0 and holding.open_sell_order_price <> sell_price:
            cancel_open_sell_orders(sec, holding)
        if sell_price <> None:
            place_sell_order(context, sec, sell_price, holding)

def clear_positions(context, data):
    context.already_stopped = True

    oo = get_open_orders()
    if len(oo) <> 0:
        for stock, orders in oo.iteritems():
            for order in orders:
                cancel_order(order)

    for sec in context.portfolio.positions:
        order_target(sec, 0)
""" END Methods dealing with positions, buying and selling according to data. """

""" Update stop lines. """
def calculate_stop_lines(context, data):
    if context.already_stopped:
        return
    if context.cur_minute < context.min_max_window + context.cool_out_time:
        return

    price_historys = data.history(context.security, "price", context.min_max_window + context.trading_minutes_interval - 1, "1m")
    mid_place = int(context.min_max_window/2)
    for sec in context.security:
        if not sec in context.stop_lines_up:
            context.stop_lines_up[sec] = {}
        if not sec in context.stop_lines_down:
            context.stop_lines_down[sec] = {}

        for i in range(context.trading_minutes_interval):
            price_history = price_historys[sec].append([])[i : i - context.trading_minutes_interval]
            price_mid = round(price_history[mid_place], 2)
            if price_mid == max(price_history):
                if price_mid not in context.stop_lines_up[sec]:
                    context.stop_lines_up[sec][price_mid] = [0, 0]
                context.stop_lines_up[sec][price_mid][0] += 1
            elif price_mid == min(price_history):
                if price_mid not in context.stop_lines_down[sec]:
                    context.stop_lines_down[sec][price_mid] = [0, 0]
                context.stop_lines_down[sec][price_mid][0] += 1
            elif price_mid > min(price_history[:mid_place]) and price_mid < max(price_history[mid_place + 1:]):
                if price_mid in context.stop_lines_up[sec]:
                    context.stop_lines_up[sec][price_mid][1] += 1
            elif price_mid < max(price_history[:mid_place]) and price_mid > min(price_history[mid_place + 1:]):
                if price_mid in context.stop_lines_down[sec]:
                    context.stop_lines_down[sec][price_mid][1] += 1

        ordered_up_lines = OrderedDict(sorted(context.stop_lines_up[sec].items(), key=lambda t: t[0]))
        ordered_down_lines = OrderedDict(sorted(context.stop_lines_down[sec].items(), key=lambda t: t[0], reverse=True))

        context.ordered_up_lines_confidence[sec] = [(t[0], round((t[1][0] + 1) * math.log(t[1][0] + 1) / (t[1][1] + 1), 2)) for t in ordered_up_lines.items()]
        context.ordered_down_lines_confidence[sec] = [(t[0], round((t[1][0] + 1) * math.log(t[1][0] + 1) / (t[1][1] + 1), 2)) for t in ordered_down_lines.items()]

        up_lines_max_confidence = OrderedDict(sorted(context.ordered_up_lines_confidence[sec], key=lambda t: t[1], reverse=True))
        down_lines_max_confidence = OrderedDict(sorted(context.ordered_down_lines_confidence[sec], key=lambda t: t[1], reverse=True))

        if len(up_lines_max_confidence) > 0:
            context.confidence_bar_up[sec] = up_lines_max_confidence.items()[int(len(up_lines_max_confidence)*context.max_confidence_proportion)][1]
        else:
            context.confidence_bar_up[sec] = 0

        if len(down_lines_max_confidence) > 0:
            context.confidence_bar_down[sec] = down_lines_max_confidence.items()[int(len(down_lines_max_confidence)*context.max_confidence_proportion)][1]
        else:
            context.confidence_bar_down[sec] = 1000000

    """ Could be used to print stop lines for debugging.
    print "=== Up Stop Lines ==="
    for (price, confidence) in ordered_up_lines_confidence:
        if confidence >= confidence_bar_up:
            print (price, confidence)
    print "=== Down Stop Lines ==="
    for (price, confidence) in ordered_down_lines_confidence:
        if confidence >= confidence_bar_down:
            print (price, confidence)
    """
""" END Update stop lines. """

""" Implementing built in interfaces, initializing at the start of the algo and everyday. """
def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    # Constants
    context.MAX_NUMBER = 10000000

    # Program settings
    set_commission(commission.PerTrade(cost=0.00))
    set_slippage(slippage.FixedSlippage(spread=0.00))
    set_long_only()

    # Portfolio settings
    context.max_portfolio_size = 10000
    context.max_position_num = 5

    # Trading params
    context.min_max_window = 10
    context.cool_out_time = 20
    context.max_confidence_proportion = 0.25

    # === Scheduling functions ===
    # The interval for each buy, sell, data operations
    # Min value suggested: 3
    context.trading_minutes_interval = 3
    trading_hours_total = 5
    trading_minutes_total = int((trading_hours_total * 60) - (context.trading_minutes_interval * 2))
    for minutez in xrange(
        context.cool_out_time * 3,
        trading_minutes_total,
        context.trading_minutes_interval
    ):
        schedule_function(buy_rebalance, date_rules.every_day(), time_rules.market_open(minutes=minutez))
    for minutez in xrange(
        context.cool_out_time * 3 + context.trading_minutes_interval - 1,
        trading_minutes_total + context.trading_minutes_interval - 1,
        context.trading_minutes_interval
    ):
        schedule_function(sell_rebalance, date_rules.every_day(), time_rules.market_open(minutes=minutez))
    for minutez in xrange(
        context.cool_out_time * 3 - 1,
        trading_minutes_total - 1,
        context.trading_minutes_interval
    ):
        schedule_function(calculate_stop_lines, date_rules.every_day(), time_rules.market_open(minutes=minutez))
    schedule_function(clear_positions, date_rules.every_day(), time_rules.market_close(hours=0, minutes=23))

def before_trading_start(context, data):
    """
    Called every day before market open.
    """
    # These are the securities that we are interested in trading each day.
    context.security = [symbol('PLUG')]

    # Initialize data related to stop lines
    context.stop_lines_up = {}
    context.stop_lines_down = {}
    context.ordered_up_lines_confidence = {}
    context.ordered_down_lines_confidence = {}
    context.confidence_bar_up = {}
    context.confidence_bar_down = {}

    #Initialize daily params
    context.cur_minute = 0
    context.cur_holdings = {}
    context.already_stopped = False

def handle_data(context, data):
    """
    Called every minute.
    """
    context.cur_minute += 1
""" END Implementing built in interfaces, initializing at the start of the algo and everyday. """
