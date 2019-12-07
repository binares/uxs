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
            if self.xs.ob['force_create'] is not None: #and not xs.ob['receives_snapshot']:
                cfg['on_activate'].append(self._init_orderbooks)
                
        self.ids_count = defaultdict(int)
    
    
    def send_orderbook(self, orderbook):
        self.create_orderbook(orderbook)
    
    
    def send_update(self, update, force_push=None):
        """
        :param force_push: if True, push everything up to the latest update,
                           overriding all on hold ('__hold__') updates
        """
        updates = [update] if isinstance(update, dict) else update
        symbols = set()
        holds = defaultdict(list)
        receives_snapshot = self.xs.ob['receives_snapshot']
        force_till_id = -1 if force_push else None
        
        for update in updates:
            symbol = update['symbol']
            ob = self.xs.orderbooks.get(symbol)
            if ob is not None or not receives_snapshot:
                self._add_to_cache(update)
                symbols.add(symbol)
                self._resolve_hold(update, holds)
            
        for symbol in symbols:
            if self.xs.orderbooks.get(symbol) is None:
                if self.xs.sh.is_subscribed_to(('orderbook',symbol), active=None):
                    self._schedule_orderbook_creation(symbol)
            else:
                for hold,id in holds[symbol]:
                    asyncio.ensure_future(
                                self._push_cache_after(symbol, hold, id))
                self._push_cache(symbol, force_till_id)
    
    
    """def send_update_as_range(self, symbol, start, end, changes):
        self._add_to_orderbook_cache(symbol, start, end, changes)"""
    
    
    def create_orderbook(self, symbol_or_ob):
        if isinstance(symbol_or_ob, str):
            return asyncio.ensure_future(self._fetch_and_create(symbol_or_ob))
        
        ob = symbol_or_ob
        self._assign(ob)
            
        f = asyncio.Future()
        f.set_result(None)
        
        return f
    
    
    async def _fetch_and_create(self, symbol):
        try:
            fetch_limit = self.xs.cis.get_value('orderbook','fetch_limit')
            args = (fetch_limit,) if fetch_limit is not None else ()
            tlogger.debug('{} - creating orderbook {}.'.format(self.xs.name, symbol))
            #self.orderbooks[symbol] = await self.xs.api.fetch_order_book(symbol)
            fetched = await self.xs.fetch_order_book(symbol,*args)
            ob = dict({'symbol': symbol}, **fetched)
        except Exception as e:
            logger2.error(e)
            logger.exception(e)
        else:
            self._assign(ob)
            
    
    def _assign(self, ob):
        symbol = ob['symbol']
        
        if 'nonce' not in ob:
            ob['nonce'] = None
        #Should the differences between old and new ob be sent to callbacks?
        self.xs.orderbooks[symbol] = ob
        #print('nonce: {}'.format(nonce))
        is_synced, performed_update = self._push_cache(symbol)
        if is_synced and not performed_update:
            #To notify that the orderbook was in fact updated (created)
            self.xs.update_orderbooks([{'symbol': symbol,
                                        'bids': [],
                                        'asks': [],
                                        'nonce': ob['nonce']}])
        if is_synced:
            self._change_status(symbol, 1)
    
    
    def _change_status(self, symbol, status):
        if self.xs.sh.is_subscribed_to(('orderbook',symbol)):
            self.xs.sh.change_subscription_state(('orderbook',symbol), status)
    
    
    def _resolve_hold(self, update, holds):
        """:type holds: defaultdict(list)"""
        if '__hold__' not in update:
            return
        symbol = update['symbol']
        hold = update['__hold__']
        id = update['__id__']
        update['__hold_until__'] = time.time() + hold
        hs = holds[symbol]
        to = next((x for i,x in enumerate(hs) if hold <= x[0]), None)
        if to is None:
            hs.append((hold,id))
        else:
            holds[symbol] = hs[:to] + [(hold,id)]
            
        return holds
            
            
    def _add_to_cache(self, update):
        symbol = update['symbol']
        update['__id__'] = self.ids_count[symbol]
        self.ids_count[symbol] += 1
        
        cache_size = self.xs.ob['cache_size'] 
        cache = self.cache[update['symbol']]['updates']
        #update = dict(update, nonce=self.resolve_nonce(update['nonce']))
        cache.append(update)
        
        if cache_size is not None:
            while len(cache) > cache_size:
                cache.pop(0)
    
    
    def _push_cache(self, symbol, force_till_id=None):
        """
        :param force_till_id: -1: pushes everything
                              None: pushes everything that is either not held or expired
                                    until it encounters a non-expired update
                              0+: pushes everything up to (including) update with matching id,
                                  and proceeds from there (or from beginning if not found)
                                  as `force_till_id=None`
        """
        #Nonce of update entry may be given as closed range [start_nonce, end_nonce]
        ob = self.xs.orderbooks.get(symbol)
        if ob is None:
            return False, False
        
        now = time.time()
        uses_nonce = self.xs.ob['uses_nonce']
        cur_nonce = ob['nonce']
        updates = self.cache[symbol]['updates']
        to_push = {'symbol': symbol, 'bids':[], 'asks':[]}
        eligible = eligible_nonce = updates
        
        if uses_nonce:
            start_from = next((len(updates)-i for i,u in enumerate(reversed(updates))
                               if self.resolve_nonce(u['nonce'])[1] <= cur_nonce), 0)
            eligible = eligible_nonce = updates[start_from:]
        
        up_to = None
        id_loc = -1
        
        if force_till_id not in (None, -1):
            id_loc = next((i for i,u in enumerate(eligible) if u['__id__']==force_till_id), -1)
        
        if force_till_id != -1:
            #Include all that come after the id and are not held / are expired
            include = next((i for i,u in enumerate(eligible[id_loc+1:])
                            if '__hold_until__' in u and u['__hold_until__'] > now), None)
            up_to = include
        
        if up_to is not None:
            up_to += id_loc + 1
        
        if up_to is not None:
            eligible = eligible[:up_to]
        
        is_synced = True
        
        def _is_synced(n0, cur_nonce, n1):
            return n0 <= cur_nonce + 1 <= n1
        
        for u in eligible:
            n0,n1 = self.resolve_nonce(u.get('nonce'))
            if uses_nonce and n1 <= cur_nonce:
                continue
            #print('({}) {} {}'.format(cur_nonce,n0,n1))
            if uses_nonce and not _is_synced(n0, cur_nonce, n1):
                is_synced = False
                #print(cur_nonce,(n0,n1),u)
                logger.debug('{} - orderbook {} nonce is unsynced with cache'.format(self.xs.name, symbol))
                self._change_status(symbol, 0)
                if self._is_orderbook_reload_time(symbol):
                    logger.debug('{} - reloading orderbook {} due to unsynced nonce.'.format(self.xs.name, symbol))
                    self._schedule_orderbook_creation(symbol)
            for side in ('bids','asks'):
                to_push[side] += u[side]
            cur_nonce = n1
        
        to_push['nonce'] = cur_nonce
        performed_update = False
        
        #If not synced should the orderbook be updated?
        if cur_nonce != ob['nonce'] or to_push.get('bids') or to_push.get('asks'):
            performed_update = True
            self.xs.update_orderbooks([to_push])
        #print(ob['nonce'])
        
        if not uses_nonce:
            #Not dropping them would result in them being re-counted as "eligible_nonce" afterwards
            self.cache[symbol]['updates'] = eligible_nonce[up_to:] if up_to is not None else []
        
        return is_synced, performed_update
    
    
    async def _push_cache_after(self, symbol, hold, force_till_id=None):
        if hold != 0:
            await asyncio.sleep(hold)
        self._push_cache(symbol, force_till_id)
    
           
    def _schedule_orderbook_creation(self, symbol):
        if self._is_orderbook_reload_time(symbol):
            future = self.create_orderbook(symbol)
            future.t_created = time.time()
            self.cache[symbol]['last_create_execution'] = future
    
    
    def _is_orderbook_reload_time(self, symbol):
        future = self.cache[symbol]['last_create_execution']
        #return future is None or time.time() > future.t_created + self.xs.ob['reload_after']
        return future is None or future.done() and time.time() > future.t_created + self.xs.ob['reload_after']
    
    
    async def _init_orderbooks(self, cnx):
        """
        Force create orderbooks (that haven't already been automatically created on 1st .send_update/.send_ob) 
        X seconds after cnx activation
        """
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
                call_via_loop_afut(self._fetch_and_create, (symbol,), loop=self.xs.loop)
    
    @staticmethod
    def resolve_nonce(nonce):
        if not hasattr(nonce,'__iter__'):
            return (nonce,nonce)
        return nonce
        
    @property
    def send_ob(self):
        return self.send_orderbook