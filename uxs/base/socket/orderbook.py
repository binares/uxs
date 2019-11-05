import asyncio
from collections import defaultdict
import time

from fons.aio import call_via_loop_afut
import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)


class OrderbookMaintainer:
    def __init__(self, xs):
        """:type xs: ExchangeSocket"""
        self.xs = xs
        def cache_item():
            return {'updates': [], 'last_create_execution': None}
        self.cache = defaultdict(cache_item)
                
        cfg = xs.connection_defaults
        for i,cfg in enumerate([xs.connection_defaults] + list(xs.connection_profiles.values())):
            if i and 'on_activate' not in cfg:
                continue
            if cfg.get('on_activate') is None:
                cfg['on_activate'] = []
            elif isinstance(cfg['on_activate'], str):
                cfg['on_activate'] = [cfg['on_activate']]
            else:
                cfg['on_activate'] = list(cfg['on_activate'])
            cfg['on_activate'].append(self._init_orderbooks)
        
    def send_orderbook(self, orderbook):
        asyncio.ensure_future(
            self.create_orderbook(orderbook))
    
    def send_update(self, update):
        updates = [update] if isinstance(update, dict) else update
        symbols = set()
        for update in updates:
            self._add_to_cache(update)
            symbols.add(update['symbol'])
            
        for symbol in symbols:
            if self.xs.orderbooks.get(symbol) is None:
                if self.xs.sh.is_subscribed_to(('orderbook',symbol), active=None):
                    self._schedule_orderbook_creation(symbol)
            else:
                self._push_cache(symbol)
        
    """def send_update_as_range(self, symbol, start, end, changes):
        self._add_to_orderbook_cache(symbol, start, end, changes)"""
                
    async def create_orderbook(self, symbol_or_ob):
        fetch_limit = self.xs.cis.get_value('orderbook','fetch_limit')
        args = (fetch_limit,) if fetch_limit is not None else ()
        try:
            if isinstance(symbol_or_ob,str):
                symbol = symbol_or_ob
                tlogger.debug('{} - creating orderbook {}.'.format(self.xs.name, symbol))
                #self.orderbooks[symbol] = await self.xs.api.fetch_order_book(symbol)
                ob = await self.xs.fetch_order_book(symbol,*args)
            else:
                ob = symbol_or_ob
                symbol = ob['symbol']
            if ob is not None:
                #Should the differences between old and new ob be sent to callbacks?
                self.xs.orderbooks[symbol] = ob
                #print('nonce: {}'.format(nonce))
                is_synced = self._push_cache(symbol)
                if is_synced:
                    self._change_status(symbol, 1)
        except Exception as e:
            logger2.error(e)
            logger.exception(e)
            
    def _change_status(self, symbol, status):
        if self.xs.sh.is_subscribed_to(('orderbook',symbol)):
            self.xs.sh.change_subscription_state(('orderbook',symbol), status)
            
    def _add_to_cache(self, update):
        cache = self.cache[update['symbol']]['updates']
        #update = dict(update, nonce=self.resolve_nonce(update['nonce']))
        cache.append(update)
        while len(cache) > self.xs.ob['cache_size']:
            cache.pop(0)
        
    def _push_cache(self, symbol):
        #Nonce of update entry may be given as closed range [start_nonce, end_nonce]
        ob = self.xs.orderbooks[symbol]
        cur_nonce = ob['nonce']
        updates = self.cache[symbol]['updates']
        to_push = {'symbol': symbol, 'bid':[], 'ask':[]}
        
        start_from = next((len(updates)-i for i,u in enumerate(reversed(updates)) 
                           if self.resolve_nonce(u['nonce'])[1] <= cur_nonce), 0)
        eligible = updates[start_from:] #updates
        is_synced = True
        
        def _is_synced(n0, cur_nonce, n1):
            return n0 <= cur_nonce + 1 <= n1
        
        for u in eligible:
            n0,n1 = self.resolve_nonce(u['nonce'])
            if n1 <= cur_nonce:
                continue
            #print('({}) {} {}'.format(cur_nonce,n0,n1))
            if not _is_synced(n0, cur_nonce, n1):
                is_synced = False
                #print(cur_nonce,(n0,n1),u)
                logger.debug('{} - orderbook {} nonce is unsynced with cache'.format(self.xs.name, symbol))
                self._change_status(symbol, 0)
                if self._is_orderbook_reload_time(symbol):
                    logger.debug('{} - reloading orderbook {} due to unsynced nonce.'.format(self.xs.name, symbol))
                    self._schedule_orderbook_creation(symbol)
            for side in ('bid','ask'):
                to_push[side] += u[side]
            cur_nonce = n1
        
        #If not synced should the orderbook be updated?
        to_push['nonce'] = cur_nonce
        self.xs.update_orderbooks([to_push])
        #print(ob['nonce'])
        
        return is_synced
                    
    def _schedule_orderbook_creation(self, symbol):
        if self._is_orderbook_reload_time(symbol):
            future = call_via_loop_afut(self.create_orderbook, (symbol,))
            future.t_created = time.time()
            self.cache[symbol]['last_create_execution'] = future
    
    def _is_orderbook_reload_time(self, symbol):
        future = self.cache[symbol]['last_create_execution']
        #return future is None or time.time() > future.t_created + self.xs.ob['reload_after']
        return future is None or future.done() and time.time() > future.t_created + self.xs.ob['reload_after']
    
    async def _init_orderbooks(self, cnx):
        """Force create orderbooks (that haven't already been automatically created on 1st .send_update/.send_ob) 
           X seconds after cnx activation"""
        wait = self.xs.ob['force_create']
        if wait is None:
            return
        await asyncio.sleep(wait)

        for s in self.xs.sh.subscriptions:
            symbol = s.params.get('symbol')
            if s.channel != 'orderbook' or s.cnx != cnx: continue
            elif not s.state and self._is_orderbook_reload_time(symbol):
                #asyncio.ensure_future(self.fetch_order_book(s['symbol']))
                logger.debug('{} - force creating orderbook {}'.format(self.xs.name, symbol))
                call_via_loop_afut(self.create_orderbook, (symbol,), loop=self.xs.loop)
    
    @staticmethod
    def resolve_nonce(nonce):
        if not hasattr(nonce,'__iter__'):
            return (nonce,nonce)
        return nonce
        
    @property
    def send_ob(self):
        return self.send_orderbook