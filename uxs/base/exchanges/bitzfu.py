# -*- coding: utf-8 -*-

# PLEASE DO NOT EDIT THIS FILE, IT IS GENERATED AND WILL BE OVERWRITTEN:
# https://github.com/ccxt/ccxt/blob/master/CONTRIBUTING.md#how-to-contribute-code

from ccxt.bitz import bitz
import math
from ccxt.base.errors import ExchangeError
from ccxt.base.errors import ArgumentsRequired
from ccxt.base.decimal_to_precision import TICK_SIZE


class bitzfu(bitz):

    def describe(self):
        return self.deep_extend(super(bitzfu, self).describe(), {
            'id': 'bitzfu',
            'name': 'Bit-Z Futures',
            'has': {
                'createMarketOrder': True,
                'fetchClosedOrders': True,
                'fetchDeposits': False,
                'fetchMyTrades': True,
                'fetchOHLCV': True,
                'fetchOpenOrders': True,
                'fetchOrder': True,
                'fetchOrders': False,
                'fetchTickers': True,
                'fetchWithdrawals': False,
            },
            'timeframes': {
                '1m': '1m',
                '5m': '5m',
                '15m': '15m',
                '30m': '30m',
                '1h': '1h',
                '4h': '4h',
                '1d': '1d',
            },
            'urls': {
                'api': {
                    'public': 'https://{hostname}',
                    'private': 'https://{hostname}',
                },
                'fees': 'https://swap.bitz.top/pub/fees',
            },
            'api': {
                'public': {
                    'get': [
                        '{}Coin',
                        '{}Kline',
                        '{}OrderBook',
                        '{}TradesHistory',
                        '{}Tickers',
                    ],
                },
                'private': {
                    'get': [
                        # '{}MyTrades',  # self is should probably be POST instead
                    ],
                    'post': [
                        'add{}Trade',
                        'cancel{}Trade',
                        'get{}ActivePositions',
                        'get{}AccountInfo',
                        'get{}MyPositions',
                        'get{}OrderResult',
                        'get{}Order',
                        'get{}TradeResult',
                        'get{}MyHistoryTrade',
                        'get{}MyTrades',
                    ],
                },
            },
            'fees': {
                'trading': {
                    'tierBased': False,
                    'percentage': True,
                    'maker': None,
                    'taker': None,
                },
            },
            'precisionMode': TICK_SIZE,
            'options': {
                'defaultLeverage': 2,  # 2, 5, 10, 15, 20, 50, 100
                'defaultIsCross': 1,  # 1: cross, -1: isolated
            },
        })

    def fetch_markets(self, params={}):
        response = self.publicGetCoin(params)
        #
        #  {
        #      "status": 200,
        #      "msg": "",
        #      "data": [
        #          {                                                                                                  # BZ settled contract:
        #              "contractId": "101",                    # contract id                                          # "201"
        #              "symbol": "BTC",                        # symbol                                               # "BTC"
        #              "settleAnchor": "USDT",                 # settle anchor                                        # "BZ"
        #              "quoteAnchor": "USDT",                  # quote anchor                                         # "USDT"
        #              "contractAnchor": "BTC",                # contract anchor                                      # "BZ \/ 1 USDT"
        #              "contractValue": "0.00100000",          # contract face value
        #              "pair": "BTC_USDT",                     # pair                                                 # "BTC_USDT"
        #              "expiry": "0000-00-00 00:00:00",        # delivery day(non-perpetual contract)
        #              "maxLeverage": "100",                   # max leverage
        #              "maintanceMargin": "0.00500000",        # maintenance margin
        #              "makerFee": "-0.00030000",              # maker fee
        #              "takerFee": "0.00070000",               # taker fee
        #              "settleFee": "0.00070000",              # settlement fee
        #              "priceDec": "1",                        # floating point decimal of price
        #              "anchorDec": "2",                       # floating point decimal of quote anchor
        #              "status": "1",                          # status，1: trading, 0: pending, -1: permanent stop
        #              "isreverse": "-1",                      # 1:reverse contract，-1: forward contract              # "-1"
        #              "allowCross": "1",                      # Allow cross position，1:Yes，-1:No
        #              "allowLeverages": "2,5,10,15,20,50,100",// Leverage multiple allowed by the system
        #              "maxOrderNum": "50",                    # max order number
        #              "maxAmount": "5000",                    # min order amount
        #              "minAmount": "1",                       # min order amount
        #              "maxPositionAmount": "5000"             # max position amount
        #          }
        #      ],
        #      "time": 1562059174,
        #      "microtime": "0.05824800 1562059174",
        #      "source": "api"
        #  }
        #
        markets = self.safe_value(response, 'data')
        result = []
        for i in range(0, len(markets)):
            market = markets[i]
            id = self.safe_string(market, 'contractId')  # unique
            pairId = self.safe_string(market, 'pair')    # NOT unique
            baseId = self.safe_string(market, 'symbol')
            quoteId = self.safe_string(market, 'quoteAnchor')
            settleId = self.safe_string(market, 'settleAnchor')
            maker = self.safe_float(market, 'makerFee')
            taker = self.safe_float(market, 'takerFee')
            base = baseId.upper()
            quote = quoteId.upper()
            settle = settleId.upper()
            base = self.safe_currency_code(base)
            quote = self.safe_currency_code(quote)
            settle = self.safe_currency_code(settle)
            symbol = base + '/' + quote
            # To preserve the uniqueness of all symbols
            if (settle != base) and (settle != quote):
                symbol = settle + '_' + symbol
            lotSize = 1.0
            if settle != 'BZ':
                lotSize = self.safe_float(market, 'contractValue')
            precision = {
                'amount': 1.0,
                'price': self.safe_integer(market, 'priceDec'),
            }
            if precision['price'] is not None:
                precision['price'] = math.pow(10, -precision['price'])
                # Only(BZ_)BTC/USD(T) and (BZ_)ETH/USD(T) markets are tick sized
                if (base == 'BTC') or (base == 'ETH'):
                    precision['price'] *= 5
            active = (self.safe_integer(market, 'status') == 1)
            isReverse = (self.safe_integer(market, 'isreverse') == 1)
            type = 'swap' if isReverse else 'future'
            future = (type == 'future')
            swap = (type == 'swap')
            result.append({
                'info': market,
                'id': id,
                'pairId': pairId,
                'symbol': symbol,
                'base': base,
                'quote': quote,
                'settle': settle,
                'baseId': baseId,
                'quoteId': quoteId,
                'settleId': settleId,
                'spot': False,
                'future': future,
                'swap': swap,
                'prediction': False,
                'type': type,
                'taker': taker,
                'maker': maker,
                'active': active,
                'precision': precision,
                'lotSize': lotSize,
                'limits': {
                    'amount': {
                        'min': self.safe_float(market, 'minAmount'),
                        'max': self.safe_float(market, 'maxAmount'),
                    },
                    'price': {
                        'min': precision['price'],
                        'max': None,
                    },
                    'cost': {
                        'min': None,
                        'max': None,
                    },
                },
            })
        return result

    def fetch_balance(self, params={}):
        self.load_markets()
        response = self.privatePostGetAccountInfo(params)
        #
        #  {
        #      "status": 200,
        #      "msg": "",
        #      "data": {
        #          "time": 1557928650,                     # time
        #          "estimate_BTC": "8.00667445",           # Total Equity(BTC)
        #          "estimate_USDT": "17000.00",            # Total Equity(USDT)
        #          "estimate_CNY": "0.00"                  # Total Equity(CNY)
        #          "balances": [
        #              {
        #                  "coin": "BTC",                  # coin
        #                  "balance": "8.00000000",        # balance
        #                  "positionMargin": "0.00635670",  # position margin
        #                  "orderMargin": "0.00000000",    # order margin
        #                  "unrlzPnl": "0.00031774",       # unrealized  profits and losses
        #                  "total": "8.00667445",          # total evaluation of the coin
        #                  "estimate_BTC": "8.00667445",   # total evaluation(BTC)
        #                  "estimate_USDT": "0.00",        # total evaluation(USDT)
        #                  "estimate_CNY": "0.00"          # total evaluation(CNY)
        #              },
        #              ...
        #          ]
        #      },
        #      "time": 1533035297,
        #      "microtime": "0.41892000 1533035297",
        #      "source": "api"
        #  }
        #
        balances = self.safe_value(response['data'], 'balances')
        result = {'info': response}
        for i in range(0, len(balances)):
            balance = balances[i]
            currencyId = self.safe_string(balance, 'coin')
            code = self.safe_currency_code(currencyId)
            account = self.account()
            account['used'] = None
            account['total'] = self.safe_float(balance, 'balance')
            account['free'] = None
            result[code] = account
        return self.parse_balance(result)

    def parse_ticker(self, ticker, market=None):
        #
        #  {
        #        "contractId": "101",                      # contract ID
        #        "pair": "BTC_USD",                        # pair
        #        "min": "8550.0",                          # 24H lowest price
        #        "max": "8867.5",                          # 24H highest price
        #        "latest": "8645.0",                       # latest price
        #        "change24h": "-0.0248",                   # 24H change
        #        "amount": "286231.00",                    # amount
        #        "volumn": "2502462.46",                   # volumn
        #        "baseAmount": "286.231 BTC",              # amount(BTC)
        #        "quoteVolumn": "2502462.46 USDT",         # amount(USDT)
        #        "openInterest": "10110336.00",            # open interest
        #        "baseOpenInterest": "10110.34 BTC",       # open interest(BTC)
        #        "quoteOpenInterest": "89623073.47 USDT",  # open interest(USDT)
        #        "indexPrice": "8065.25",                  # index price
        #        "fairPrice": "8060.09",                   # fair price
        #        "nextFundingRate": "-0.00075000"          # next funding rate
        #  }
        #
        timestamp = None
        symbol = None
        if market is None:
            marketId = self.safe_string(ticker, 'contactId')
            market = self.safe_value(self.markets_by_id, marketId)
        if market is not None:
            symbol = market['symbol']
        last = self.safe_float(ticker, 'latest')
        percentage = self.safe_float(ticker, 'change24h')
        open = None
        change = None
        average = None
        if (percentage is not None) and (last is not None) and (symbol is not None):
            open = float(self.price_to_precision(symbol, last / (1 + percentage)))
            change = float(self.price_to_precision(symbol, last - open))
            average = self.sum(open, last) / 2
        if percentage is not None:
            percentage *= 100
        baseVolume = None
        quoteVolume = None
        baseAmount = self.safe_string(ticker, 'baseAmount')
        quoteAmount = self.safe_string(ticker, 'quoteVolumn')
        if baseAmount is not None:
            baseVolumeLoc = baseAmount.find(' ')
            if baseVolumeLoc != -1:
                baseVolume = float(baseAmount[0:baseVolumeLoc])
        if quoteAmount is not None:
            quoteVolumeLoc = quoteAmount.find(' ')
            if quoteVolumeLoc != -1:
                quoteVolume = float(quoteAmount[0:quoteVolumeLoc])
        vwap = None
        if quoteVolume is not None:
            if baseVolume is not None:
                if baseVolume > 0:
                    vwap = quoteVolume / baseVolume
        return {
            'symbol': symbol,
            'timestamp': timestamp,
            'datetime': self.iso8601(timestamp),
            'high': self.safe_float(ticker, 'max'),
            'low': self.safe_float(ticker, 'min'),
            'bid': None,
            'bidVolume': None,
            'ask': None,
            'askVolume': None,
            'vwap': vwap,
            'open': open,
            'close': last,
            'last': last,
            'previousClose': None,
            'change': change,
            'percentage': percentage,
            'average': average,
            'baseVolume': baseVolume,
            'quoteVolume': quoteVolume,
            'info': ticker,
        }

    def fetch_ticker(self, symbol, params={}):
        tickers = self.fetch_tickers([symbol])
        ticker = self.safe_value(tickers, symbol)
        if ticker is None:
            raise ExchangeError(self.id + ' - ticker ' + symbol + ' could not be fetched')
        return ticker

    def fetch_tickers(self, symbols=None, params={}):
        self.load_markets()
        request = {}
        if (symbols is not None) and (len(symbols) == 1):
            id = self.market_id(symbols[0])
            request['contractId'] = id
        response = self.publicGetTickers(self.extend(request, params))
        #
        #  {
        #    "status": 200,
        #    "msg": "",
        #    "data": [{
        #        "contractId": "101",                      # contract ID
        #        "pair": "BTC_USD",                        # pair
        #        "min": "8550.0",                          # 24H lowest price
        #        "max": "8867.5",                          # 24H highest price
        #        "latest": "8645.0",                       # latest price
        #        "change24h": "-0.0248",                   # 24H change
        #        "amount": "286231.00",                    # amount
        #        "volumn": "2502462.46",                   # volumn
        #        "baseAmount": "286.231 BTC",              # amount(BTC)
        #        "quoteVolumn": "2502462.46 USDT",         # amount(USDT)
        #        "openInterest": "10110336.00",            # open interest
        #        "baseOpenInterest": "10110.34 BTC",       # open interest(BTC)
        #        "quoteOpenInterest": "89623073.47 USDT",  #open interest(USDT)
        #        "indexPrice": "8065.25",                  #index price
        #        "fairPrice": "8060.09",                   #fair price
        #        "nextFundingRate": "-0.00075000"          #next funding rate
        #      },
        #      ...
        #    ],
        #    "time": 1573813113,
        #    "microtime": "0.23065700 1573813113",
        #    "source": "api"
        #  }
        #
        tickers = self.safe_value(response, 'data')
        timestamp = self.parseMicrotime(self.safe_string(response, 'microtime'))
        result = {}
        for i in range(0, len(tickers)):
            ticker = tickers[i]
            id = self.safe_string(ticker, 'contractId')
            pairId = self.safe_string(ticker, 'pair')
            market = None
            if id in self.markets_by_id:
                market = self.markets_by_id[id]
            ticker = self.parse_ticker(ticker, market)
            symbol = ticker['symbol']
            if symbol is None:
                if market is not None:
                    symbol = market['symbol']
            if (symbol is not None) and ((symbols is None) or self.in_array(symbol, symbols)):
                result[symbol] = self.extend(ticker, {
                    'timestamp': timestamp,
                    'datetime': self.iso8601(timestamp),
                })
        return result

    def fetch_order_book(self, symbol, limit=None, params={}):
        self.load_markets()
        #  Parameter 	    Required    Type 	Comment
        #  contractId 	    Yes 	    int 	Contract ID
        #  depth 	        no 	        string 	Depth type 5, 10, 15, 20, 30, 100,,default10
        request = {
            'contractId': self.market_id(symbol),
        }
        if limit is not None:
            request['limit'] = limit
        response = self.publicGetOrderBook(self.extend(request, params))
        #  {
        #      "status":200,
        #      "msg":"",
        #      "data":{
        #            "bids":[
        #              {
        #                 "price": "8201.32",
        #                 "amount": "2820"
        #              },
        #              ...
        #          ],
        #          "asks":[
        #              {
        #                  "price": "8202.14",
        #                  "amount": "4863"
        #              },
        #              ...
        #          ]
        #      }
        #      "time": 1532671288,
        #      "microtime": "0.23065700 1532671288",
        #      "source": "api"
        #  }
        orderbook = self.safe_value(response, 'data')
        timestamp = self.parseMicrotime(self.safe_string(response, 'microtime'))
        return self.parse_order_book(orderbook, timestamp, 'bids', 'asks', 'price', 'amount')

    def parse_trade(self, trade, market=None):
        #
        # fetchTrades(public)
        #
        #      {
        #          "time": 1558432920,                 # timestamp
        #          "price": "7926.41",                 # price
        #          "num": 7137,                        # number
        #          "type": "buy"                       # type : "buy" / "sell"
        #      }
        #
        # fetchMyTrades(private)
        #
        #      {
        #          "tradeId": "6534702673362395142",   # Deal ID
        #          "contractId": "1",                  # Contract ID
        #          "pair": "BTC_USD",                  # Contract Market
        #          "price": "8000.00",                 # Transaction Price
        #          "num": "500",                       # Number of transactions
        #          "type": "buy",                      # Type of transaction
        #          "tradeFee": "0.00001250",           # Transaction charges
        #          "leverage": "10",                   # Gearing
        #          "isCross": "-1",                    # Whether the warehouse is full or not，1:Yes,-1:No
        #          "time": 1557994526                  # Dealing time
        #      }
        #
        id = self.safe_string(trade, 'tradeId')
        timestamp = self.safe_timestamp(trade, 'time')
        contractId = self.safe_string(trade, 'contractId')
        pairId = self.safe_string(trade, 'pair')
        symbol = None
        if (market is None) and (contractId is not None):
            market = self.safe_value(self.markets_by_id, contractId)
        if market is not None:
            symbol = market['symbol']
        elif (symbol is None) and (pairId is not None):
            baseId, quoteId = pairId.split('_')
            base = self.safe_currency_code(baseId)
            quote = self.safe_currency_code(quoteId)
            symbol = base + '/' + quote
            if int(contractId) >= 200:
                symbol = 'BZ_' + symbol
        side = self.safe_string(trade, 'type')
        price = self.safe_float(trade, 'price')
        amount = self.safe_float(trade, 'num')
        cost = amount
        fee = None
        if market is not None:
            tradeFee = self.safe_float(trade, 'tradeFee')
            if tradeFee is not None:
                tradeFee *= -1
                base, quote = symbol.split('/')
                currency = base if market['swap'] else quote
                fee = {
                    'cost': tradeFee,
                    'currency': currency,
                }
        return {
            'timestamp': timestamp,
            'datetime': self.iso8601(timestamp),
            'symbol': symbol,
            'id': id,
            'order': None,
            'type': 'limit',
            'side': side,
            'takerOrMaker': None,
            'price': price,
            'amount': amount,
            'cost': cost,
            'fee': fee,
            'info': trade,
        }

    def fetch_trades(self, symbol, since=None, limit=None, params={}):
        self.load_markets()
        market = self.market(symbol)
        #  Parameter 	    Required 	Type 	Comment
        #  contractId 	    Yes 	    int 	Contract ID
        #  pageSize 	    No 	        int 	Get data volume range:10-300 default 10
        request = {
            'contractId': market['id'],
        }
        if limit is not None:
            request['pageSize'] = min(max(limit, 10), 300)
        response = self.publicGetTradesHistory(self.extend(request, params))
        #  {
        #      "status": 200,
        #      "msg": "",
        #      "data": {
        #            "lists": [
        #              {
        #                  "time": 1558432920,     #timestamp
        #                  "price": "7926.41",     #price
        #                  "num": 7137,            # number
        #                  "type": "buy"           # type
        #              }
        #          ]
        #      }
        #      "time": 1532671288,
        #      "microtime": "0.23065700 1532671288",
        #      "source": "api"
        #  }
        lists = self.safe_value(response['data'], 'lists')
        if lists is None:
            return []
        return self.parse_trades(lists, market, since, limit)

    def parse_ohlcv(self, ohlcv, market=None, timeframe='1m', since=None, limit=None):
        #  [
        #      "1558433100000",        # time
        #      "7921.69000000",        # opening price
        #      "7921.96000000",        # highest price
        #      "7882.31000000",        # lowest price
        #      "7882.31000000",        # closing price
        #      "1793940.00000000",     # volume
        #      "14183930623.27000000"  # turnover
        #  ]
        return [
            int(ohlcv[0]),
            float(ohlcv[1]),
            float(ohlcv[2]),
            float(ohlcv[3]),
            float(ohlcv[4]),
            float(ohlcv[5]),
        ]

    def fetch_ohlcv(self, symbol, timeframe='1m', since=None, limit=None, params={}):
        self.load_markets()
        market = self.market(symbol)
        #
        #  Parameter 	Required 	Type 	    Comment
        #  contractId 	Yes 	    int 	    Contract ID
        #  type 	    Yes 	    string 	    K line type 1m, 5m, 15m, 30m, 1h, 4h, 1d，default5m
        #  size 	    No 	        int 	    Get data volume 1-300, default 300
        #
        request = {
            'contractId': market['id'],
            'type': self.timeframes[timeframe],
        }
        if limit is not None:
            request['size'] = min(limit, 300)  # 1-300
        response = self.publicGetKline(self.extend(request, params))
        #  {
        #      "status": 200,
        #      "msg": "",
        #      "data": {
        #            "lists": [
        #              [
        #                  "1558433100000",        #time
        #                  "7921.69000000",        #opening price
        #                  "7921.96000000",        #highest price
        #                  "7882.31000000",        #lowest price
        #                  "7882.31000000",        #closing price
        #                  "1793940.00000000",     #volume
        #                  "14183930623.27000000"  #turnover
        #              ]
        #          ]
        #      }
        #      "time": 1532671288,
        #      "microtime": "0.23065700 1532671288",
        #      "source": "api"
        #  }
        lists = self.safe_value(response['data'], 'lists', None)
        if lists is None:
            return []
        return self.parse_ohlcvs(lists, market, timeframe, since, limit)

    def parse_order_status(self, status):
        statuses = {
            '-1': 'canceled',
            '0': 'open',
            '1': 'closed',
        }
        return self.safe_string(statuses, status, status)

    def parse_order(self, order, market=None):
        #
        # fetchOrders
        #
        #      {
        #          "orderId": "734709",    # orderID
        #          "contractId": "101",    # contract ID
        #          "amount": "500",        # amount
        #          "price": "7500.00",     # price
        #          "type": "limit",        # type，limit: limit order，market: market order
        #          "leverage": "10",       # leverage
        #          "direction": "1",       # direction，1:long，-1:short
        #          "orderStatus": "0",     # status，0:unfinished，1:finished，-1:cancelled
        #          "isCross": "-1",        # Is Cross，1:Yes,-1:No
        #          "available": "500",     # available order 
        #          "time": 1557994750,     # time
        #          "pair": "BTC_USD"       # pair
        #      }
        #
        id = self.safe_string(order, 'orderId')
        symbol = None
        if market is None:
            marketId = self.safe_string(order, 'contractId')
            market = self.safe_value(self.markets_by_id, marketId)
        if market is not None:
            symbol = market['symbol']
        type = self.safe_string(order, 'type')
        side = self.safe_integer(order, 'direction')
        if side is not None:
            side = 'buy' if (side == 1) else 'sell'
        price = self.safe_float(order, 'price')
        if price == 0:
            price = None  # market order
        amount = self.safe_float(order, 'amount')
        remaining = self.safe_float(order, 'available')
        filled = None
        if (amount is not None) and (remaining is not None):
            filled = max(0, amount - remaining)
        timestamp = self.safe_integer(order, 'time')
        if timestamp is not None:
            timestamp *= 1000
        cost = None
        if (price is not None) and (filled is not None) and (market is not None):
            if market['swap']:
                cost = filled
            else:
                cost = filled * price
            cost *= market['lotSize']
        status = self.parse_order_status(self.safe_string(order, 'orderStatus'))
        return {
            'id': id,
            'datetime': self.iso8601(timestamp),
            'timestamp': timestamp,
            'lastTradeTimestamp': None,
            'status': status,
            'symbol': symbol,
            'type': type,
            'side': side,
            'price': price,
            'cost': cost,
            'amount': amount,
            'filled': filled,
            'remaining': remaining,
            'trades': None,
            'fee': None,
            'info': order,
        }

    def create_order(self, symbol, type, side, amount, price=None, params={}):
        self.load_markets()
        #
        #  Parameter 	Required    Type 	Comment
        #  contractId 	Yes 	    int 	Contract ID
        #  price 	    No 	        float 	Price，if type is market，delivery of Price is not required.
        #  amount 	    Yes 	    int 	Amount
        #  leverage 	Yes 	    float 	Leverage
        #  direction 	Yes 	    int 	Direction 1: long；-1: short
        #  type 	    Yes 	    string 	Tpye limit: limited order; market: market price order, require lowercase string
        #  isCross 	    Yes 	    int 	Cross Margin or not 1: Cross Margin，-1：Isolated Margin
        #
        market = self.market(symbol)
        ordDirection = 1 if (side == 'buy') else -1
        request = {
            'contractId': market['id'],
            'amount': self.amount_to_precision(symbol, amount),
            'leverage': self.options['defaultLeverage'],
            'direction': ordDirection,
            'type': type,
            'isCross': self.options['defaultIsCross'],
        }
        if price is not None:
            request['price'] = price
        response = self.privatePostAddTrade(self.extend(request, params))
        #
        #  {
        #      "status": 200,
        #      "msg": "",
        #      "data":{
        #           "orderId": 710370
        #      },
        #      "time": 1533035297,
        #      "microtime": "0.41892000 1533035297",
        #      "source": "api"
        #  }
        #
        order = self.safe_value(response, 'data', {})
        order['time'] = self.safe_integer(response, 'time')
        order = self.parse_order(order, market)
        return self.extend(order, {
            'type': type,
            'side': side,
            'amount': amount,
            'price': price,
        })

    def cancel_order(self, id, symbol=None, params={}):
        self.load_markets()
        request = {
            'entrustSheetId': id,
        }
        response = self.privatePostCancelTrade(self.extend(request, params))
        #
        #  {
        #      "status": 200,
        #      "msg": "",
        #      "data": {
        #
        #      },
        #      "time": 1533035297,
        #      "microtime": "0.41892000 1533035297",
        #      "source": "api"
        #  }
        #
        return response

    def fetch_order(self, id, symbol=None, params={}):
        self.load_markets()
        request = {
            'entrustSheetIds': id,
        }
        response = self.privatePostGetOrderResult(self.extend(request, params))
        # {
        #      "status":200,
        #      "msg":"",
        #      "data":{
        #          [
        #              "orderId": "734709",             # order ID
        #              "contractId": "101",             # contract ID
        #              "pair": "BTC_USD",               # pair
        #              "amount": "500",                 # amount
        #              "price": "7500.00",              # price
        #              "type": "limit",                 # type，limit: limit order，market: market order
        #              "leverage": "10",                # leverage
        #              "direction": "1",                # direction，1:long，-1:short
        #              "orderStatus": "-1",             # status，0:unfinished，1:finished，-1:cancelled
        #              "available": "500",              # available order
        #              "time": 1557994750               # time
        #          ]
        #      },
        #      "time": 1533035297,
        #      "microtime": "0.41892000 1533035297",
        #      "source": "api"
        #  }
        orders = self.safe_value(response, 'data')
        if (orders is None) or (len(orders) == 0):
            raise ExchangeError(self.id + ' - order ' + id + ' could not be fetched')
        return self.parse_order(orders[0])

    def fetch_orders_with_method(self, method, symbol=None, since=None, limit=None, params={}):
        if symbol is None:
            raise ArgumentsRequired(self.id + ' - ' + method + ' requires a symbol argument')
        apiMethods = {
            'fetchClosedOrders': 'privatePostGetMyHistoryTrade',
            'fetchOpenOrders': 'privatePostGetOrder',
        }
        apiMethod = apiMethods[method]
        self.load_markets()
        market = self.market(symbol)
        request = {
            'contractId': market['id'],
        }
        if apiMethod == 'privatePostGetMyHistoryTrade':
            request['page'] = 1  # required integer, 1-10
            if limit is not None:
                request['pageSize'] = limit
            else:
                request['pageSize'] = 50  # required integer, max 50
        response = getattr(self, apiMethod)(self.extend(request, params))
        #
        #  {
        #      "status": 200,
        #      "msg": "",
        #      "data": [
        #           {
        #             "orderId": "734709",             # orderID
        #              "contractId": "101",            # contract ID
        #              "amount": "500",                # amount
        #              "price": "7500.00",             # price
        #              "type": "limit",                # type，limit: limit order，market: market order
        #              "leverage": "10",               # leverage
        #              "direction": "1",               # direction，1:long，-1:short
        #              "orderStatus": "0",             # status，0:unfinished，1:finished，-1:cancelled
        #              "isCross": "-1",                # Is Cross，1:Yes,-1:No
        #              "available": "500",             # available order 
        #              "time": 1557994750,             # time
        #              "pair": "BTC_USD"               # pair
        #          }
        #      ],
        #      "time": 1533035297,
        #      "microtime": "0.41892000 1533035297",
        #      "source": "api"
        #  }
        #
        orders = self.safe_value(response, 'data', [])
        return self.parse_orders(orders, market, since, limit)

    def fetch_closed_orders(self, symbol=None, since=None, limit=None, params={}):
        return self.fetch_orders_with_method('fetchClosedOrders', symbol, since, limit, params)

    def fetch_open_orders(self, symbol=None, since=None, limit=None, params={}):
        return self.fetch_orders_with_method('fetchOpenOrders', symbol, since, limit, params)

    def fetch_my_trades(self, symbol=None, since=None, limit=None, params={}):
        if symbol is None:
            raise ArgumentsRequired(self.id + ' - fetchMyTrades requires a symbol argument')
        self.load_markets()
        market = self.market(symbol)
        #
        #  Parameter 	    Required 	Type 	Comment
        #  contractId 	    Yes 	    int 	Contract ID
        #  page 	        No 	        int 	Default 1 rang:1-10
        #  pageSize 	    No 	        int 	Default 50 rang:1-50
        #  createDate 	    No 	        int 	Date(Day), Default 7, currently only 7 or 30 are supported.
        #
        request = {
            'contractId': market['id'],
            'page': 1,
        }
        if limit is not None:
            request['pageSize'] = limit
        response = self.privatePostGetMyTrades(self.extend(request, params))
        #
        #  {
        #      "status":200,
        #      "msg":"",
        #      "data": [
        #          {
        #              "tradeId": "6534702673362395142",   # Deal ID
        #              "contractId": "1",                  # Contract ID
        #              "pair": "BTC_USD",                  # Contract Market
        #              "price": "8000.00",                 # Transaction Price
        #              "num": "500",                       # Number of transactions
        #              "type": "buy",                      # Type of transaction
        #              "tradeFee": "0.00001250",           # Transaction charges
        #              "leverage": "10",                   # Gearing
        #              "isCross": "-1",                    # Whether the warehouse is full or not，1:Yes,-1:No
        #              "time": 1557994526                  # Dealing time
        #          }
        #      ],
        #      "time":1533035297,
        #      "microtime":"0.41892000 1533035297",
        #      "source":"api"
        #  }
        #
        trades = self.safe_value(response, 'data', [])
        return self.parse_trades(trades, market, since, limit, params)

    def sign(self, path, api='public', method='GET', params={}, headers=None, body=None):
        baseUrl = self.implode_params(self.urls['api'][api], {'hostname': self.hostname})
        prefix = 'Market' if (api == 'public') else 'Contract'
        suffix = self.implode_params(path, {'': 'Contract'})
        if method == 'GET':
            suffix = 'get' + suffix
        url = baseUrl + '/' + prefix + '/' + suffix
        query = None
        if api == 'public':
            query = self.urlencode(params)
            if len(query):
                url += '?' + query
        else:
            self.check_required_credentials()
            body = self.rawencode(self.keysort(self.extend({
                'apiKey': self.apiKey,
                'timeStamp': self.seconds(),
                'nonce': self.nonce(),
            }, params)))
            body += '&sign=' + self.hash(self.encode(body + self.secret))
            headers = {'Content-type': 'application/x-www-form-urlencoded'}
        return {'url': url, 'method': method, 'body': body, 'headers': headers}