import asyncio
import aiohttp
import functools
import json
import datetime,time
dt = datetime.datetime
td = datetime.timedelta

from uxs.base.socket import ExchangeSocket

from fons.time import ctime_ms
import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)

class kraken(ExchangeSocket):
    exchange = 'kraken'
    base_url = 'ws.kraken.com'
    #base_url = 'ws-beta.kraken.com'
    ping_interval = 30
    #subscription_push_rate_limit = 0.04
    #max_nr_subscriptions = 100
    #reload_orderbook_after = 15
    #reload_orderbook_sequence_missing = 1
    #match_is_subset_of_market = True
    has = {
        'ticker': {'bid': True, 'ask': True, 'bidVolume': True, 'askVolume': True},     
        'all_tickers': False,
        'orderbook': True,
        'market': True,#{'match': True},
        'account': False,
        'match': False,
        'fetch_tickers': True,
        'fetch_ticker': True,
        'fetch_order_book': True,
        'fetch_balance': True,
    }
    #message_id_keyword = 'id'
    #update_order_executed_on_trade = True
    #update_order_payout_on_trade = True
    #update_order_left_on_trade = True
    #disable_auto_subscription_state_enabling = {
    #    'orderbook': True,
    #}
        
    def handle(self,r):
        pass
            
            
    def encode(self,config):
        id0 = config['_']
        unsub = True if id0.startswith('-') else False
        id = id0[1:] if unsub else id0
        event = 'subscribe' if not unsub else 'unsubscribe'
        
        _map = {
            'ticker': 'ticker',
            'orderbook': 'book',
            'market': 'trade',
            #'x':'spread',
            #'xx': 'ohlc,
        }
        subscription_name = _map[id]
        
        symbol = config['symbol']
        symbol_list = [symbol] if isinstance(symbol,str) else list(symbol)
        pair = [self.convert_symbol(x,1) for x in symbol_list]
        
        req = {
          "event": event,
          "pair": pair,
          "subscription": dict({
            "name": subscription_name,
          }, **({x:y for x,y in config if x not in ('_','symbol')}
                if not unsub else {}))
        }
        
        return (req, None)
    
        
    def convert_symbol(self,symbol,direction=1):
        #0: ex to ccxt 1: ccxt to ex
        if not direction:
            return '/'.join(self.convert_cy(x,0) for x in symbol.split('/'))
        else:
            return '/'.join(self.convert_cy(x,1) for x in symbol.split('/'))
        
    async def ping(self):
        await self._socket.send(json.dumps({
            "event": "ping",
            "reqid": ctime_ms()
        }))
