# -*- coding: utf-8 -*-

# PLEASE DO NOT EDIT THIS FILE, IT IS GENERATED AND WILL BE OVERWRITTEN:
# https://github.com/ccxt/ccxt/blob/master/CONTRIBUTING.md#how-to-contribute-code

from ccxt.base.exchange import Exchange
import hashlib
from ccxt.base.errors import ExchangeError
from ccxt.base.errors import AuthenticationError
from ccxt.base.errors import InsufficientFunds
from ccxt.base.errors import InvalidOrder
from ccxt.base.errors import OrderNotFound
from ccxt.base.errors import NotSupported
from ccxt.base.errors import DDoSProtection
from ccxt.base.decimal_to_precision import TICK_SIZE


class gateiofu(Exchange):

    def describe(self):
        return self.deep_extend(super(gateiofu, self).describe(), {
            'id': 'gateiofu',
            'name': 'Gate.io Futures',
            'countries': ['CN'],
            'rateLimit': 1000,
            'version': 'v4',
            'has': {
                'CORS': False,
                'createMarketOrder': False,
                'fetchTicker': False,
                'fetchTickers': False,
                'fetchOrderBook': False,
                'withdraw': False,
                'fetchDeposits': False,
                'fetchWithdrawals': False,
                'fetchTransactions': False,
                'createDepositAddress': False,
                'fetchDepositAddress': False,
                'fetchClosedOrders': False,
                'fetchTrades': False,
                'fetchOHLCV': True,
                'fetchOpenOrders': False,
                'fetchOrderTrades': False,
                'fetchOrders': False,
                'fetchOrder': False,
                'fetchMyTrades': False,
            },
            'timeframes': {
                '10s': '10s',
                '1m': '1m',
                '5m': '5m',
                '15m': '15m',
                '30m': '30m',
                '1h': '1h',
                '4h': '4h',
                '8h' : '8h',
                '1d': '1d',
                '1w': '7d',
            },
            'urls': {
                'logo': 'https://user-images.githubusercontent.com/1294454/31784029-0313c702-b509-11e7-9ccc-bc0da6a0e435.jpg',
                'api': {
                    'public': 'https://fx-api.gateio.ws/api',
                    'private': 'https://fx-api.gateio.ws/api',
                },
                'www': 'https://gate.io/',
                'doc': 'https://www.gate.io/docs/futures/api/index.html#gate-api-v4',
                'fees': [
                    'https://gate.io/fee',
                    'https://support.gate.io/hc/en-us/articles/115003577673',
                ],
                'referral': 'https://www.gate.io/signup/2436035',
            },
            'api': {
                'public': {
                    'get': [
                        'futures/{settle}/contracts',
                        'futures/{settle}/contracts/{contract}',
                        'futures/{settle}/order_book',
                        'futures/{settle}/trades',
                        'futures/{settle}/candlesticks',
                        'futures/{settle}/tickers',
                        'futures/{settle}/funding_rate',
                        'futures/{settle}/insurance',
                    ],
                },
                'private': {
                    'get': [
                        'futures/{settle}/accounts',
                        'futures/{settle}/account_book',
                        'futures/{settle}/positions',
                        'futures/{settle}/positions/{contract}',
                        'futures/{settle}/orders',
                        'futures/{settle}/orders/{order_id}',
                        'futures/{settle}/my_trades',
                        'futures/{settle}/position_close',
                        'futures/{settle}/liquidates',
                        'futures/{settle}/price_orders',
                        'futures/{settle}/price_orders/{order_id}',
                    ],
                    'post': [
                        'futures/{settle}/positions/{contract}/margin',
                        'futures/{settle}/positions/{contract}/leverage',
                        'futures/{settle}/positions/{contract}/risk_limit',
                        'futures/{settle}/orders',
                        'futures/{settle}/price_orders',
                    ],
                    'delete': [
                        'futures/{settle}/orders',
                        'futures/{settle}/orders/{order_id}',
                        'futures/{settle}/price_orders',
                        'futures/{settle}/price_orders/{order_id}',
                    ],
                },
            },
            'fees': {
                'trading': {
                    'tierBased': True,
                    'percentage': True,
                    'maker': -0.00025,
                    'taker': 0.00075,
                },
            },
            'exceptions': {
                'exact': {
                    '4': DDoSProtection,
                    '5': AuthenticationError,  # {result: "false", code:  5, message: "Error: invalid key or sign, please re-generate it from your account"}
                    '6': AuthenticationError,  # {result: 'false', code: 6, message: 'Error: invalid data  '}
                    '7': NotSupported,
                    '8': NotSupported,
                    '9': NotSupported,
                    '15': DDoSProtection,
                    '16': OrderNotFound,
                    '17': OrderNotFound,
                    '20': InvalidOrder,
                    '21': InsufficientFunds,
                },
                # https://gate.io/api2#errCode
                'errorCodeNames': {
                    '1': 'Invalid request',
                    '2': 'Invalid version',
                    '3': 'Invalid request',
                    '4': 'Too many attempts',
                    '5': 'Invalid sign',
                    '6': 'Invalid sign',
                    '7': 'Currency is not supported',
                    '8': 'Currency is not supported',
                    '9': 'Currency is not supported',
                    '10': 'Verified failed',
                    '11': 'Obtaining address failed',
                    '12': 'Empty params',
                    '13': 'Internal error, please report to administrator',
                    '14': 'Invalid user',
                    '15': 'Cancel order too fast, please wait 1 min and try again',
                    '16': 'Invalid order id or order is already closed',
                    '17': 'Invalid orderid',
                    '18': 'Invalid amount',
                    '19': 'Not permitted or trade is disabled',
                    '20': 'Your order size is too small',
                    '21': 'You don\'t have enough fund',
                },
            },
            'precisionMode': TICK_SIZE,
            'options': {
                'settleCurrencyIds': [
                    'btc',
                    'usdt',
                ],
                #'limits': {
                #   'cost': {
                #        'min': {
                #           'BTC': 0.0001,
                #            'ETH': 0.001,
                #            'USDT': 1,
                #        },
                #    },
                #},
            },
        })

    def fetch_markets(self, params={}):
        settleCurrencyIds = self.options['settleCurrencyIds']
        settleCurrencyIdsByMarketId = {}
        markets = []
        for i in range(0, len(settleCurrencyIds)):
            settleCurrencyId = settleCurrencyIds[i]
            query = self.omit(params, 'type')
            query['settle'] = settleCurrencyId
            response = self.publicGetFuturesSettleContracts(query)
            if not  (isinstance(response, list)):
                raise ExchangeError(self.id + ' fetchMarkets got an unrecognized response')
            for j in range(0, len(response)):
                market = response[j]
                settleCurrencyIdsByMarketId[market['name']] = settleCurrencyId
            markets = self.array_concat(markets, response)
        result = []
        for i in range(0, len(markets)):
            market = markets[i]
            id = market['name']
            settleCurrencyId = settleCurrencyIdsByMarketId[id]
            settleCurrency = self.safeCurrencyCode(settleCurrencyId.upper())
            # all of their symbols are separated with an underscore
            # but not boe_eth_eth(BOE_ETH/ETH) which has two underscores
            # https://github.com/ccxt/ccxt/issues/4894
            parts = id.split('_')
            numParts = len(parts)
            baseId = parts[0]
            quoteId = parts[1]
            if numParts > 2:
                baseId = parts[0] + '_' + parts[1]
                quoteId = parts[2]
            base = self.safe_currency_code(baseId)
            quote = self.safe_currency_code(quoteId)
            symbol = base + '/' + quote
            type = 'swap' if (market['type'] == 'inverse') else 'future'
            spot = False
            swap = (type == 'swap')
            future = (type == 'future')
            maker = self.safeFloat(market, 'maker_fee_rate')
            taker = self.safeFloat(market, 'taker_fee_rate')
            precision = {
                'amount': market['order_size_min'],
                'price': self.safeFloat(market, 'order_price_round'),
            }
            amountLimits = {
                'min': market['order_size_min'],
                'max': market['order_size_max'],
            }
            priceLimits = {
                'min': precision['price'],
                'max': None,
            }
            defaultCost = amountLimits['min'] * priceLimits['min']
            minCost = defaultCost
            #minCost = self.safe_float(self.options['limits']['cost']['min'], quote, defaultCost)
            costLimits = {
                'min': minCost,
                'max': None,
            }
            limits = {
                'amount': amountLimits,
                'price': priceLimits,
                'cost': costLimits,
            }
            active = True
            result.append({
                'id': id,
                'symbol': symbol,
                'base': base,
                'quote': quote,
                'baseId': baseId,
                'quoteId': quoteId,
                'active': active,
                'maker': maker,
                'taker': taker,
                'precision': precision,
                'limits': limits,
                'type': type,
                'spot': spot,
                'future': future,
                'swap': swap,
                'settleCurrency': settleCurrency,
                'settleCurrencyId': settleCurrencyId,
                'info': market,
            })
        return result

    def parse_ohlcv(self, ohlcv, market=None, timeframe='1m', since=None, limit=None):
        return [
            ohlcv['t'] * 1000,
            float(ohlcv['o']),
            float(ohlcv['h']),
            float(ohlcv['l']),
            float(ohlcv['c']),
            float(ohlcv['v']),
        ]

    def fetch_ohlcv(self, symbol, timeframe='1m', since=None, limit=None, params={}):
        self.load_markets()
        market = self.market(symbol)
        # Return specified contract candlesticks. 
        # If prefix contract with mark_, the contract's mark price candlesticks are returned
        # if prefix with index_, index price candlesticks will be returned.
        request = {
            'settle': market['settleCurrencyId'],
            'contract': market['id'],
            'interval': self.timeframes[timeframe],
        }
        # Maximum of 2000 points are returned in one query.
        limit = min(limit, 2000) if (limit is not None) else limit
        if since is not None:
            request['from'] = self.truncate(since / 1000)
            if limit is not None:
                periodDurationInSeconds = self.parse_timeframe(timeframe)
                # The period must not exceed 2000 points
                limitFinal = limit if (limit != 2000) else (limit - 1)
                request['to'] = request['from'] + (limitFinal *  periodDurationInSeconds)
        elif limit is not None:
            request['limit'] = limit
        response = self.publicGetFuturesSettleCandlesticks(self.extend(request, params))
        #
        #  [
        #      {
        #          "t": 1539852480,
        #          "v": 97151,
        #          "c": "1.032",
        #          "h": "1.032",
        #          "l": "1.032",
        #          "o": "1.032"
        #      }
        #  ]
        #
        return self.parse_ohlcvs(response, market, timeframe, since, limit)

    def sign(self, path, api='public', method='GET', params={}, headers=None, body=None):
        prefix = (api + '/') if (api == 'private') else ''
        url = self.urls['api'][api] + '/' + self.version + '/' + prefix + self.implode_params(path, params)
        query = self.omit(params, self.extract_params(path))
        if api == 'public':
            if query:
                url += '?' + self.urlencode(query)
        else:
            self.check_required_credentials()
            nonce = self.nonce()
            request = {'nonce': nonce}
            body = self.urlencode(self.extend(request, query))
            signature = self.hmac(self.encode(body), self.encode(self.secret), hashlib.sha512)
            headers = {
                'Key': self.apiKey,
                'Sign': signature,
                'Content-Type': 'application/x-www-form-urlencoded',
            }
        return {'url': url, 'method': method, 'body': body, 'headers': headers}

    def handle_errors(self, code, reason, url, method, headers, body, response, requestHeaders, requestBody):
        if response is None:
            return
        resultString = self.safe_string(response, 'result', '')
        if resultString != 'false':
            return
        errorCode = self.safe_string(response, 'code')
        message = self.safe_string(response, 'message', body)
        if errorCode is not None:
            feedback = self.safe_string(self.exceptions['errorCodeNames'], errorCode, message)
            self.throw_exactly_matched_exception(self.exceptions['exact'], errorCode, feedback)
