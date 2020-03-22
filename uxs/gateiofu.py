import ccxt
import time
from uxs.base.socket import ExchangeSocket
from uxs.fintls.ob import infer_side

import fons.log

logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)


class gateiofu(ExchangeSocket):
    exchange = 'gateiofu'
    url_components = {
        'ws': 'wss://fx-ws.gateio.ws/v4/ws',
        'test': 'wss://fx-ws-testnet.gateio.ws/v4/ws',
    }
    channel_defaults = {
        'url': '<$ws>/<settle>',
        'cnx_params_converter_config': {
            'currency_aliases': ['settle'],
        },
    }
    channels = {
    }
    channel_ids = {
        'orderbook': 'futures.order_book',
    }
    connection_defaults = {
    }
    has = {
        'orderbook': True,
    }
    ob = {
        #'on_unsync': 'restart',
        'uses_nonce': False, # the nonces aren't strictly linear
        'receives_snapshot': True,
        'force_create': None,
    }
    _cached_snapshots = {}
    __deepcopy_on_init__ = ['_cached_snapshots']
    
    
    def handle(self, R):
        r = R.data
        channel = r.get('channel')
        event = r.get('event')
        error = r.get('error')
        if error is not None:
            logger.debug(r)
        elif event in ('subscribe','unsubscribe'):
            logger.debug(r)
        elif channel == 'futures.order_book':
            self.on_orderbook(r)
    
    
    def on_orderbook(self, r):
        """
        SNAPSHOT:
        {
            'time': 1584746782,
            'channel': 'futures.order_book',
            'event': 'all',
            'error': None,
            'result': {
                'contract': 'BTC_USDT',
                'asks': [{'p': '6140', 's': 3200}, {'p': '6144', 's': 36134}, ...],
                'bids': [{'p': '6130.1', 's': 20000}, {'p': '6132.1', 's': 29008}, ...],
        }
        UPDATE:
        {
            'time': 1584746782,
            'channel': 'futures.order_book',
            'event': 'update',
            'error': None,'
            'result': [
                {'p': '6133.7', 's': 12000,'c': 'BTC_USDT', 'id': 475526246},
                {'p': '6142.9', 's': 0, 'c': 'BTC_USDT', 'id': 475526247},
                {'p': '6134.3', 's': 0, 'c': 'BTC_USDT', 'id': 475526248},
                ...
                {'p': '6155.2', 's': -10000, 'c': 'BTC_USDT', 'id': 475526257}
                ...
            ]
        }
        """
        rr = r['result']
        # event: "all" / "update"
        is_snap = r['event']=='all'
        timestamp = r['time'] * 1000
        if is_snap:
            parsed = {'symbol': self.convert_symbol(rr['contract'], 0)}
            parsed.update(
                self.api.parse_order_book(rr, timestamp, price_key='p', amount_key='s'))
            # The snapshot doesn't have a nonce yet, cache it until the arrival of first update
            #self._cached_snapshots[parsed['symbol']] = (parsed, time.time())
            self.ob_maintainer.send_orderbook(parsed)
        else:
            parsed_updates = []
            for update in rr:
                parsed_updates.append(
                    self.parse_ob_update(update))
            parsed_updates.sort(key=lambda x: x['nonce'])
            #self._release_cached_snapshots(parsed_updates)
            self.ob_maintainer.send_update(parsed_updates)
    
    
    def parse_ob_update(self, update):
        symbol = self.convert_symbol(update['c'], 0)
        price = float(update['p'])
        amount = float(update['s'])
        
        if amount < 0:
            side = 'asks'
        elif amount == 0:
            #ob = self.orderbooks[symbol]
            #side = infer_side(ob, price)
            side = 'unassigned'
        else:
            side = 'bids'
        
        return {
            'symbol': symbol,
            side: [[price, abs(amount)]],
            'nonce': update['id'],
        }
    
    
    def _release_cached_snapshots(self, updates):
        now = time.time()
        for update in updates:
            ob, t_created = self._cached_snapshots.pop(update['symbol'], (None, None))
            # It is reasonable to expect the first update within such period
            if t_created and now - t_created < 100:
                ob['nonce'] = update['nonce'] - 1
                self.ob_maintainer.send_orderbook(ob)
    
    
    def transform(self, params):
        kw = {}
        if 'symbol' in params:
            cls = type(params['symbol'])
            settle = []
            symbols = [params['symbol']] if issubclass(cls, str) else params['symbol']
            for symbol in symbols:
                if self.markets and symbol in self.markets:
                    settle.append(self.markets[symbol]['settleCurrency'])
                else:
                    base, quote = symbol.split('/')
                    settle.append('USDT' if quote.lower()=='usdt' else 'BTC')
            kw['settle'] = settle[0] if issubclass(cls, str) else cls(settle)
        
        return dict(params, **kw)
    
    
    def encode(self, rq, sub=None):
        channel = rq.channel
        params = rq.params
        required = self.get_value(channel, 'required')
        #timeframe = params.get('timeframe','')
        channel_id = self.channel_ids[channel]
        event = 'subscribe' if sub else 'unsubscribe'
        
        payload = []
        if 'symbol' in required:
            payload += [self.convert_symbol(params['symbol'], 1)]
        if channel=='orderbook':
            limit = 20
            if params.get('limit') is not None:
                limit = next(x for x in (20, 10, 5, 1) if params['limit']>=x)
            payload += [str(limit), "0"]
            #limit     String     Yes     limit, legal limits: 20, 10, 5, 1
            #interval     String     Yes     legal intervals: "0"
        
        return {
            'time' : ccxt.Exchange.seconds(),
            'channel' : channel_id,
            'event': event,
            'payload' : payload # ["BTC_USD", "20", "0"]
         }
