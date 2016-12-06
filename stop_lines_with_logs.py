from collections import OrderedDict
from quantopian.pipeline import Pipeline
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.data import morningstar
from quantopian.pipeline.factors import SimpleMovingAverage, AverageDollarVolume
from quantopian.pipeline.filters.morningstar import IsPrimaryShare
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
        self.open_buy_order_number += number

    def cancel_open_buy_order_and_update(self, number_canceled):
        self.cash += number_canceled * self.open_buy_order_price
        self.num_stocks += self.open_buy_order_number - number_canceled
        self.open_buy_order_price = 0
        self.open_buy_order_number = 0

    def order_sell(self, number, price):
        print "holding change: sell " + str(number) + " of " + str(self.security) + " at " + str(price)
        self.open_sell_order_price = price
        self.open_sell_order_number += number

    def cancel_open_sell_order_and_update(self, number_canceled):
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
    amount_canceled = get_open_buy_order_amount(sec, True)
    holding.cancel_open_buy_order_and_update(amount_canceled)

def cancel_open_sell_orders(sec, holding):
    amount_canceled = get_open_sell_order_amount(sec, True)
    holding.cancel_open_sell_order_and_update(amount_canceled)

def get_open_buy_order_amount(sec, cancel):
    amount_open = 0
    oos = get_open_orders(sec)
    for order in oos:
        if 0 < order.amount: #it is a buy order
            print "STATUS: open buy order: " + str(order.amount) + " of " + str(sec) + " | filled: " + str(order.filled)
            if cancel:
                print "ACTION: CANCEL"
                cancel_order(order)
            amount_open += order.amount - order.filled
    return amount_open

def get_open_sell_order_amount(sec, cancel):
    amount_open = 0
    oos = get_open_orders(sec)
    for order in oos:
        if 0 > order.amount: #it is a sell order
            print "STATUS: open sell order: " + str(order.amount) + " of " + str(sec) + " | filled: " + str(order.filled)
            if cancel:
                print "ACTION: CANCEL"
                cancel_order(order)
            amount_open -= order.amount - order.filled
    return amount_open
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

    for i in range(len(context.security)):
        num_to_hold = context.max_position_num - len(context.cur_holdings)
        if num_to_hold == 0:
            break
        sec = context.today_candidate.next()
        if sec in context.cur_holdings:
            continue
        holding_cash = context.cash_today / num_to_hold
        context.cur_holdings[sec] = Holding(sec, holding_cash)
        context.cash_today -= holding_cash

    for sec in context.cur_holdings:
        holding = context.cur_holdings[sec]
        cur_price = round(float(data.current([sec], 'price')), 2)
        my_cost = context.MAX_NUMBER
        if sec in context.portfolio.positions:
            my_cost = round(float(context.portfolio.positions[sec].cost_basis), 2)

        buy_price = getBuyLineBelowPrice(context, sec, min(cur_price, my_cost))

        if holding.open_buy_order_price <> 0 and holding.open_buy_order_price <> buy_price:
            cancel_open_buy_orders(sec, holding)
        if holding.open_buy_order_number <> 0 and get_open_buy_order_amount(sec, False) == 0:
            cancel_open_buy_orders(sec, holding)
        if buy_price <> None:
            place_buy_order(sec, buy_price, holding)

def sell_rebalance(context, data):
    if context.already_stopped:
        return

    for sec in context.security:
        if not sec in context.cur_holdings:
            continue

        holding = context.cur_holdings[sec]
        cur_price = round(float(data.current([sec], 'price')), 2)
        my_cost = None
        if sec in context.portfolio.positions:
            my_cost = round(float(context.portfolio.positions[sec].cost_basis), 2)

        sell_price = getSellLineAbovePrice(context, sec, cur_price)

        if my_cost <> None and cur_price < my_cost:
            possible_buy_price = getBuyLineBelowPrice(context, sec, cur_price)
            if possible_buy_price == None:
                sell_price = cur_price

        if sell_price == None and cur_price > my_cost:
            # This could be more strategic, but for now, we just sell at the current price to maintain profit
            sell_price = cur_price

        if my_cost <> None and sell_price <> None and sell_price - 0.01 > my_cost:
            sell_price -= 0.01

        if holding.open_sell_order_price <> 0 and holding.open_sell_order_price <> sell_price and sell_price <> None:
            cancel_open_sell_orders(sec, holding)
        if holding.open_sell_order_number <> 0 and get_open_sell_order_amount(sec, False) == 0:
            cancel_open_sell_orders(sec, holding)
        if sell_price <> None:
            place_sell_order(context, sec, sell_price, holding)

        if holding.num_stocks == 0 and holding.open_sell_order_number == 0 and holding.open_buy_order_number == 0:
            context.cash_today += holding.cash
            del context.cur_holdings[sec]

def clear_positions(context, data):
    print "ACTION: CLEAR POSITIONS"
    for sec in context.security:
        if sec in context.cur_holdings:
            holding = context.cur_holdings[sec]
            cancel_open_buy_orders(sec, holding)
            cancel_open_sell_orders(sec, holding)
            context.cash_today += holding.cash
        # Just to assure that all open orders are canceled
        oos = get_open_orders(sec)
        for order in oos:
            cancel_order(order)
    print "PORTFOLIO: day end CASH: " + str(context.cash_today)
    for sec in context.security:
        if sec in context.portfolio.positions:
            amount = context.portfolio.positions[sec].amount
            print "PORTFOLIO: day end SECURITY: " + str(sec) + " AMOUNT: " + str(amount)
        order_target(sec, 0)
    context.already_stopped = True
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
            price_history_i = price_historys[sec].append([])[i : i - context.trading_minutes_interval]
            price_history = [round(x, 2) for x in price_history_i]
            price_mid = price_history[mid_place]
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

""" Pipeline getting security candidates everyday. """
def pipeline_filter_candidates(context):
    """
    Create our pipeline.
    """
    #Constants
    lowest_volume_percent = 80
    highest_volume_percent = 100
    lowest_price = 1.05
    highest_price = 2.20

    # Filter for primary share equities. IsPrimaryShare is a built-in filter.
    primary_share = IsPrimaryShare()

    # Equities listed as common stock (as opposed to, say, preferred stock).
    # 'ST00000001' indicates common stock.
    common_stock = morningstar.share_class_reference.security_type.latest.eq('ST00000001')

    # Non-depositary receipts. Recall that the ~ operator inverts filters,
    # turning Trues into Falses and vice versa
    not_depositary = ~morningstar.share_class_reference.is_depositary_receipt.latest

    # Equities not trading over-the-counter.
    not_otc = ~morningstar.share_class_reference.exchange_id.latest.startswith('OTC')

    # Not when-issued equities.
    not_wi = ~morningstar.share_class_reference.symbol.latest.endswith('.WI')

    # Equities without LP in their name, .matches does a match using a regular
    # expression
    not_lp_name = ~morningstar.company_reference.standard_name.latest.matches('.* L[. ]?P.?$')

    # Equities with a null value in the limited_partnership Morningstar
    # fundamental field.
    not_lp_balance_sheet = morningstar.balance_sheet.limited_partnership.latest.isnull()

    # Equities whose most recent Morningstar market cap is not null have
    # fundamental data and therefore are not ETFs.
    have_market_cap = morningstar.valuation.market_cap.latest.notnull()

    # At least a certain price
    price = USEquityPricing.close.latest
    AtLeastPrice   = (price >= lowest_price)
    AtMostPrice    = (price <= highest_price)

    # Filter for stocks that pass all of our previous filters.
    tradeable_stocks = (
        primary_share
        & common_stock
        & not_depositary
        & not_otc
        & not_wi
        & not_lp_name
        & not_lp_balance_sheet
        & have_market_cap
        & AtLeastPrice
        & AtMostPrice
    )

    log.info('\nAlgorithm initialized variables:\n context.DAILY_CANDIDATE_NUMBER %s \n LowVar %s \n HighVar %s'
        % (context.DAILY_CANDIDATE_NUMBER, lowest_volume_percent, highest_volume_percent)
    )

    # High dollar volume filter.
    base_universe = AverageDollarVolume(
        window_length=20,
        mask=tradeable_stocks
    ).percentile_between(lowest_volume_percent, highest_volume_percent)

    # Short close price average.
    ShortAvg = SimpleMovingAverage(
        inputs=[USEquityPricing.close],
        window_length=3,
        mask=base_universe
    )

    # Long close price average.
    LongAvg = SimpleMovingAverage(
        inputs=[USEquityPricing.close],
        window_length=45,
        mask=base_universe
    )

    percent_difference = (ShortAvg - LongAvg) / LongAvg

    stocks_worst = percent_difference.bottom(context.DAILY_CANDIDATE_NUMBER)
    securities_to_trade = (stocks_worst)

    return Pipeline(
        columns={
            'stocks_worst': stocks_worst
        },
        screen=(securities_to_trade),
    )
""" END Pipeline getting security candidates everyday. """

""" Implementing built in interfaces, initializing at the start of the algo and everyday. """
def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    # Constants
    context.MAX_NUMBER = 10000000
    context.DAILY_CANDIDATE_NUMBER = 15
    context.UNTOUCHABLE_STOCKS = [
        symbol('GDX'),
        symbol('GDXJ'),
        symbol('SLV'),
        symbol('JNUG'),
        symbol('CNY'),
        symbol('GOOG'),
        symbol('NQ'),
        symbol('SGG'),
        symbol('JO'),
        symbol('GSG'),
        symbol('UVXY'),
        symbol('CXW'),
        symbol('UCO'),
        symbol('SDS'),
        symbol('KGC'),
        symbol('DBA'),
        symbol('WMCR'),
        symbol('SPY'),
        symbol('CXW'),
        symbol('JPM'),
        symbol('FB'),
        symbol('CHAD'),
        symbol('UDN'),
        symbol('GAB'),
        symbol('GARS')
    ]

    # Program settings
    set_commission(commission.PerTrade(cost=0.00))
    set_slippage(slippage.FixedSlippage(spread=0.00))
    set_long_only()

    # Pipeline settings
    pipeline = pipeline_filter_candidates(context)
    attach_pipeline(pipeline, 'pipeline_filter_candidates')

    # Portfolio settings
    context.max_portfolio_size = 2000
    context.max_position_num = 10

    # Trading params
    context.min_max_window = 10
    context.cool_out_time = 20
    context.max_confidence_proportion = 0.25

    # === Scheduling functions ===
    # The interval for each buy, sell, data operations
    # Min value suggested: 3
    context.trading_minutes_interval = 3
    trading_hours_total = 6.5
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
    # Prepare daily security candidates
    context.output = pipeline_output('pipeline_filter_candidates')
    context.security = context.output[context.output['stocks_worst']].index.tolist()
    for sec in context.UNTOUCHABLE_STOCKS:
        if sec in context.security:
            context.security.remove(sec)
    log.info("Today's number of candidates: %s" % (len(context.security)))

    from itertools import cycle
    context.today_candidate = cycle(context.security)

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
    context.cash_today = context.max_portfolio_size

def handle_data(context, data):
    """
    Called every minute.
    """
    context.cur_minute += 1
""" END Implementing built in interfaces, initializing at the start of the algo and everyday. """
