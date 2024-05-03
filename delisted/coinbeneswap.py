from uxs.coinbene import coinbene

import fons.log

logger, logger2, tlogger, tloggers, tlogger0 = fons.log.get_standard_5(__name__)


class coinbeneswap(coinbene):
    exchange = "coinbeneswap"
    has = {
        "ticker": {"bidVolume": True, "askVolume": True},
        "account": {"position": True},
        #'fetch_tickers': {
        #    'ask': True, 'askVolume': False, 'average': True, 'baseVolume': False, 'bid': True, 'bidVolume': False,
        #    'change': True, 'close': True, 'datetime': False, 'high': True, 'last': True, 'low': True, 'open': True,
        #    'percentage': True, 'previousClose': False, 'quoteVolume': True, 'symbol': True, 'timestamp': False,
        #    'vwap': False},
        #'fetch_ticker': True,
        #'fetch_ohlcv': {'timestamp': True, 'open': True, 'high': True, 'low': True, 'close': True, 'volume': True},
        #'fetch_order_book': {'asks': True, 'bids': True, 'datetime': True, 'nonce': False, 'timestamp': True},
        #'fetch_trades': {
        #    'amount': True, 'cost': True, 'datetime': True, 'fee': False, 'id': False, 'order': False,
        #    'price': True, 'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': False},
        #'create_order': {'id': True},
        #'fetch_order': {
        #    'amount': True, 'average': True, 'clientOrderId': False, 'cost': True, 'datetime': True, 'fee': True,
        #    'filled': True, 'id': True, 'lastTradeTimestamp': False, 'price': True, 'remaining': True, 'side': True,
        #    'status': True, 'symbol': True, 'timestamp': True, 'trades': False, 'type': True},
        #'fetch_open_orders': {'symbolRequired': False},
        #'fetch_closed_orders': {'symbolRequired': False},
    }
    has["all_tickers"] = has["ticker"].copy()
    channel_ids = {
        "orderbook": "btc/orderBook.<symbol>.<limit>",
        "ohlcv": "btc/kline.<symbol>.<timeframe>",
        "trades": "btc/tradeList.<symbol>",
        "ticker": "btc/ticker.<symbol>",
        "all_tickers": "btc/ticker.all",
        "account": ["btc/user.account", "btc/user.order", "btc/user.position"],
    }

    def parse_ticker(self, r):
        """
        {
          "symbol": "BTCUSDT",
          "lastPrice": "8548.0",
          "markPrice": "8548.0",
          "bestAskPrice": "8601.0",
          "bestBidPrice": "8600.0",
          "bestAskVolume": "1222",
          "bestBidVolume": "56505",
          "high24h": "8600.0000",
          "low24h": "242.4500",
          "open24h": "8203.5500",
          "volume24h": "4994",
          "timestamp": 1584412736365
          "fundingRate": "0.000100",
          "openInterest": "",
          "openPrice": "403.05"}
        }
        """
        map = {
            "last": "lastPrice",
            "high": "high24h",
            "low": "low24h",
            "open": "open24h",
            "quoteVolume": "volume24h",
            "bid": "bestBidPrice",
            "bidVolume": "bestBidVolume",
            "ask": "bestAskPrice",
            "askVolume": "bestAskVolume",
        }
        apply = {"symbol": lambda x: self.convert_symbol(x, 0)}

        return self.api.ticker_entry(
            **self.api.lazy_parse(r, ["symbol", "timestamp"], map, apply), info=r
        )

    def parse_ohlcv(self, r):
        """
        {
          "c": 7513.01,
          "h": 7513.37,
          "l": 7510.02,
          "o": 7510.24,
          "m": 7512.03,
          "v": 60.5929,
          "t": 1578278880
        }
        """
        return [r["t"] * 1000, r["o"], r["h"], r["l"], r["c"], r["v"]]
