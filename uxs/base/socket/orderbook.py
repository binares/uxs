import asyncio
from collections import defaultdict
import time
import math

from fons.aio import call_via_loop_afut
from uxs.fintls.ob import update_branch, infer_side, create_orderbook
import fons.log

logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)


class OrderbookMaintainer:
    """Maintains orderbooks of ExchangeSocket"""
    config_key = 'ob'
    channel = 'orderbook'
    data_key = 'orderbooks'
    fetch_method = 'fetch_order_book'
    update_method = 'update_orderbooks'
    name = 'ob'
    
    
    def __init__(self, xs):
        """:type xs: ExchangeSocket"""
        self.xs = xs
        def cache_item():
            return {'updates': [],
                    'last_reload_execution': None,
                    'last_restart_execution': None,
                    'last_warned': None}
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
            if self.cfg['force_create'] is not None: #and not cfg['receives_snapshot']:
                cfg['on_activate'].append(self._init_orderbooks)
                
        self.ids_count = defaultdict(int)
        self.is_synced = defaultdict(bool)
    
    
    def send_orderbook(self, orderbook):
        self.create_orderbook(orderbook)
    
    
    def send_update(self, update, force_push=None):
        """
        :param force_push: if True, push everything up to the latest update,
                           overriding all on hold ('__hold__') updates
        """
        force_till_id = -1 if force_push else None
        symbols, holds = self.store_update(update)
        
        for symbol in symbols:
            if self.data.get(symbol) is None:
                if self.xs.sh.is_subscribed_to(self.id_tuple(symbol), active=None):
                    self._schedule_creation(symbol)
            else:
                for hold, id in holds[symbol]:
                    asyncio.ensure_future(
                                self._push_cache_after(symbol, hold, id))
                self._push_cache(symbol, force_till_id)
    
    
    def store_update(self, update):
        updates = [update] if isinstance(update, dict) else update
        symbols = set()
        holds = defaultdict(list)
        receives_snapshot = self.cfg['receives_snapshot']
        
        for update in updates:
            symbol = update['symbol']
            ob = self.data.get(symbol)
            if ob is not None or not receives_snapshot:
                self._add_to_cache(update)
                symbols.add(symbol)
                self._resolve_hold(update, holds)
                
        return symbols, holds
    
    
    """def send_update_as_range(self, symbol, start, end, changes):
        self._add_to_orderbook_cache(symbol, start, end, changes)"""
    
    
    def create_orderbook(self, symbol_or_ob):
        if isinstance(symbol_or_ob, str):
            return asyncio.ensure_future(self._fetch_and_create(symbol_or_ob))
        
        ob = self.build_ob(symbol_or_ob) # to ensure it is correctly formatted
        self._assign(ob)
            
        f = asyncio.Future()
        f.set_result(None)
        
        return f
    
    
    def build_ob(self, ob):
        return create_orderbook(ob)
    
    
    async def _fetch_and_create(self, symbol):
        try:
            fetch_limit = self.cfg['fetch_limit']
            s = self.xs.get_subscription(self.id_tuple(symbol))
            limit = self.resolve_limit(s.params.get('limit'))
            # use "null" to purposefully leave `fetch_limit` to None
            # and prevent `limit` overriding it
            if fetch_limit == 'null':
                fetch_limit = None
            elif fetch_limit is None and limit is not None:
                fetch_limit = limit
            args = (fetch_limit,) if fetch_limit is not None else ()
            tlogger.debug('{} - creating {} {}.'.format(self.xs.name, self.name, symbol))
            #self.data[symbol] = await getattr(self.api, self.fetch_method)(symbol)
            fetched = await getattr(self.xs, self.fetch_method)(symbol, *args)
            if limit is not None:
                fetched['bids'] = fetched['bids'][:limit]
                fetched['asks'] = fetched['asks'][:limit]
            rev_back = ''
            if self.cfg['uses_nonce'] and self.cfg['ignore_fetch_nonce']:
                since = time.time() - self.cfg['assume_fetch_max_age']
                cache = self.cache[symbol]['updates']
                i, item = next(((i,x) for i,x in enumerate(reversed(cache))
                                if x['time_added'] < since), (None,None))
                fetched['nonce'] = self.resolve_nonce(item['nonce'])[1] if item else -2
                rev_back = ' (rev-back: -{})'.format(i if item else 'inf')
            tlogger.debug('{} - fetched {} {} nonce {}{}'.format(
                           self.xs.name, self.name, symbol, fetched.get('nonce'), rev_back))
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
        
        if self.cfg['uses_nonce'] and ob['nonce'] is None:
            raise ValueError("{} '{}' can't be assigned because it is missing nonce"
                             .format(self.name, symbol))
        
        # Should the differences between old and new ob be sent to callbacks?
        self.data[symbol] = self._deep_overwrite(ob)
        #print('nonce: {}'.format(nonce))
        self.is_synced[symbol] = True # this will reset in ._push_cache
        is_synced, performed_update = self._push_cache(symbol) 
        if is_synced and not performed_update:
            # To notify that the orderbook was in fact updated (created)
            method = getattr(self.xs, self.update_method)
            method([{'symbol': symbol,
                     'bids': [],
                     'asks': [],
                     'nonce': ob['nonce']}],
                    enable_sub=True)
        
        if self.cfg['purge_cache_on_create']:
            # We want to start clean, in case new nonces start from 0 again
            # or if orderbook was received via fetch and doesn't have a nonce
            # (while snapshot/updates do)
            self.purge_cache(symbol)
    
    
    def _deep_overwrite(self, new_ob):
        # This ensures that the id() of orderbook dict and its bids/asks lists
        # never change, even if `del self.data[symbol]` has been evoked
        prev = self._data[new_ob['symbol']]
        prev.update({k:v for k,v in new_ob.items() if k not in ('bids','asks')})
        for k in ('bids','asks'):
            if prev.get(k) is not None:
                prev[k].clear()
            else:
                prev[k] = []
            prev[k] += new_ob[k]
            
        return prev
        
    
    def _change_status(self, symbol, status):
        if self.xs.sh.is_subscribed_to(self.id_tuple(symbol)):
            # This will also delete the orderbook (assuming delete_data_on_unsub=True)
            self.xs.sh.change_subscription_state(self.id_tuple(symbol), status, True)
    
    
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
    
    
    @staticmethod
    def infer_side(ob, price, to_push=None):
        if to_push is None:
            return infer_side(ob, price)
        model_ob = {'bids': [], 'asks': []}
        for side in ('bids','asks'):
            new_extremum = OrderbookMaintainer._play_out(ob, to_push, side)
            if 0 < new_extremum < math.inf:
                model_ob[side] = [[new_extremum, 0]]
        return infer_side(model_ob, price)
    
    
    @staticmethod
    def _play_out(ob, to_push, side):
        """Predicts the resulting outermost bid/ask after pushing the updates"""
        nullified = set()
        new_branch = []
        extremum = 0 if side=='bids' else math.inf
        op = max if side=='bids' else min
        for item in to_push[side]:
            price, _, amount = update_branch(item, new_branch, side)
            if not amount:
                nullified.add(price)
            elif price in nullified:
                nullified.remove(price)
        if new_branch:
            extremum = op(extremum, new_branch[0][0])
        ob_extremum = next((price for price,_ in ob[side] if price not in nullified), None)
        if ob_extremum is not None:
            extremum = op(extremum, ob_extremum)
        
        return extremum
        
            
    def _add_to_cache(self, update):
        symbol = update['symbol']
        update['__id__'] = self.ids_count[symbol]
        self.ids_count[symbol] += 1
        keys = ('bids','asks','unassigned')
        if all(update.get(x) is None for x in keys):
            raise ValueError('Got empty update (symbol: {})'.format(symbol))
        for x in keys:
            if update.get(x) is None:
                update[x] = []
        cache_size = self.cfg['cache_size'] 
        cache = self.cache[update['symbol']]['updates']
        update['time_added'] = time.time()
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
        # Nonce of update entry may be given as closed range [start_nonce, end_nonce]
        ob = self.data.get(symbol)
        if ob is None:
            return False, False
        
        now = time.time()
        uses_nonce = self.cfg['uses_nonce']
        nonce_increment = self.cfg['nonce_increment']
        on_unsync = self.cfg['on_unsync']
        on_unassign = self.cfg['on_unassign']
        if on_unassign is None:
            on_unassign = 'reload' if uses_nonce else 'restart'
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
            # Include all that come after the id and are not held / are expired
            include = next((i for i,u in enumerate(eligible[id_loc+1:])
                            if '__hold_until__' in u and u['__hold_until__'] > now), None)
            up_to = include
        
        if up_to is not None:
            up_to += id_loc + 1
        
        if up_to is not None:
            eligible = eligible[:up_to]
        
        def _reset(method, reason):
            if self._is_time(symbol, method):
                logger.debug('{} - {}ing {} {} due to {}.'.format(self.xs.name, method, self.name, symbol, reason))
                self._renew(symbol, method)
        
        is_synced = self.is_synced[symbol]
        
        def _is_delta_synced(n0, cur_nonce, n1):
            return n0 <= cur_nonce + nonce_increment <= n1
        
        def _is_increasing(n0, cur_nonce, n1):
            return cur_nonce < n0 <= n1
        
        if isinstance(nonce_increment, int):
            _is_synced = _is_delta_synced
        else:
            _is_synced = _is_increasing
        
        for u in eligible:
            n0,n1 = self.resolve_nonce(u.get('nonce'))
            if uses_nonce and n1 <= cur_nonce:
                continue
            #print('({}) {} {}'.format(cur_nonce,n0,n1))
            if uses_nonce and not _is_synced(n0, cur_nonce, n1):
                self.is_synced[symbol] = is_synced = False
                self._warn(symbol)
                #print(cur_nonce,(n0,n1),u)
                self._change_status(symbol, 0)
                method = on_unsync if on_unsync is not None else 'reload'
                _reset(method, 'unsynced nonce')
                return False, False
            for side in ('bids','asks'):
                to_push[side] += u[side]
            for item in u['unassigned']:
                price, amount = item[0], item[1]
                inferred_side = self.infer_side(ob, price, to_push)
                if inferred_side is None:
                    self._warn_uninferrable(symbol, item, to_push)
                    # Non-existing deletion isn't as important (it already isn't in the orderbook)
                    if amount:
                        self.purge_cache(symbol)
                        _reset(on_unassign, 'uninferrable item: {}'.format(item))
                        return False, False
                else:
                    to_push[inferred_side] += [item]
            cur_nonce = n1
        
        to_push['nonce'] = cur_nonce
        performed_update = False
        
        # If not synced should the orderbook be updated?
        if cur_nonce != ob['nonce'] or to_push.get('bids') or to_push.get('asks'):
            performed_update = True
            getattr(self.xs, self.update_method) ([to_push], enable_sub=is_synced) # update the orderbook
        #print(ob['nonce'])
        
        if not uses_nonce:
            # Not dropping them would result in them being re-counted as "eligible_nonce" afterwards
            self.cache[symbol]['updates'] = eligible_nonce[up_to:] if up_to is not None else []
        
        return is_synced, performed_update
    
    
    async def _push_cache_after(self, symbol, hold, force_till_id=None):
        if hold != 0:
            await asyncio.sleep(hold)
        self._push_cache(symbol, force_till_id)
    
    
    def _renew(self, symbol, method='reload'):
        """
        :param method: reload / restart
        """
        if method not in ('reload','restart'):
            raise ValueError('{} - incorrect ob renew method: {}'.format(self.xs.name, method))
        if method == 'reload':
            self._schedule_creation(symbol)
        else:
            self._restart_subscription(symbol)
    
    
    def _schedule_creation(self, symbol):
        if self._is_time(symbol, 'reload'):
            future = self.create_orderbook(symbol)
            future.t_created = time.time()
            self.cache[symbol]['last_reload_execution'] = future
    
    
    def _restart_subscription(self, symbol):
        
        async def unsub_and_resub(s):
            await s.unsub()
            await asyncio.sleep(0.05)
            return await s.push()
        
        if self._is_time(symbol, 'restart'):
            s = self.xs.get_subscription(self.id_tuple(symbol))
            future = asyncio.ensure_future(unsub_and_resub(s))
            future.t_created = time.time()
            self.cache[symbol]['last_restart_execution'] = future
    
    
    def _is_time(self, symbol, method='reload'):
        """:param method: reload / restart"""
        if method not in ('reload','restart'):
            raise ValueError('{} - incorrect ob renew method: {}'.format(self.xs.name, method))
        future = self.cache[symbol]['last_{}_execution'.format(method)]
        if not self.xs.is_subscribed_to(self.id_tuple(symbol), active=None):
            return False
        return future is None or future.done() and time.time() > future.t_created + self.cfg['{}_interval'.format(method)]
    
    
    def _warn(self, symbol):
        t = self.cache[symbol]['last_warned']
        if t is None or time.time() > t + self.cfg['reload_interval']:
            self.cache[symbol]['last_warned'] = time.time()
            logger.debug('{} - {} {} nonce is unsynced with cache'.format(self.xs.name, self.name, symbol))
    
    
    def _warn_uninferrable(self, symbol, item, to_push):
        ob = self.data.get(symbol)
        bids = ob['bids'][:10] if ob is not None else None
        asks = ob['asks'][:10] if ob is not None else None
        tlogger.debug('{} - {} {} encountered uninferrable item: {}\n\n'
                      'asks[:10] {}\n\nbids[:10] {}\n\nto_push: {}\n'
                      .format(self.xs.name, self.name, symbol, item, asks, bids, to_push))
    
    
    async def _init_orderbooks(self, cnx):
        """
        Force create orderbooks (that haven't already been automatically created on 1st .send_update/.send_ob) 
        X seconds after cnx activation
        """
        wait = self.cfg['force_create']
        if wait is None:
            return
        await asyncio.sleep(wait)

        for s in self.xs.sh.subscriptions:
            symbol = s.params.get('symbol')
            if s.channel != self.channel or s.cnx != cnx: continue
            elif not s.state and self._is_time(symbol, 'reload'):
                logger.debug('{} - force creating {} {}'.format(self.xs.name, self.name, symbol))
                call_via_loop_afut(self._fetch_and_create, (symbol,), loop=self.xs.loop)
    
    
    def purge_cache(self, symbol):
        if symbol in self.cache:
            self.cache[symbol]['updates'].clear()
    
    
    def resolve_limit(self, limit):
        if limit is None:
            return None
        limits = sorted(self.cfg['limits'])
        return next((x for x in limits if limit<=x), None)
    
    
    def set_limit(self, symbol, limit):
        symbols = [symbol] if isinstance(symbol, str) else symbol
        for symbol in symbols:
            s = self.xs.get_subscription(self.id_tuple(symbol))
            s.params['limit'] = limit
            if s.merger is not None:
                s.merger.params['limit'] = limit
    
    
    def get_last_cached_nonce(self, symbol, default=None):
        cache = self.cache[symbol]['updates']
        if len(cache):
            return self.resolve_nonce(cache[-1].get('nonce'))[1]
        return default
    
    
    @staticmethod
    def resolve_nonce(nonce):
        if not hasattr(nonce,'__iter__'):
            return (nonce,nonce)
        return nonce
    
    
    def id_tuple(self, symbol):
        return (self.channel, symbol)
    
    @property
    def send_ob(self):
        return self.send_orderbook
    
    @property
    def cfg(self):
        return getattr(self.xs, self.config_key)
    
    @property
    def data(self):
        return getattr(self.xs, self.data_key)
    
    @property
    def _data(self):
        return getattr(self.xs, '_'+self.data_key)
