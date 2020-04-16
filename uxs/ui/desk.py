"""
Trading desk for a single exchange.
"""
from collections import defaultdict, OrderedDict
import asyncio
import time
import sys
import random
import functools
from ccxt import TICK_SIZE

from fons.aio import call_via_loop_afut
from fons.argv import parse_argv
from fons.debug import safeAsyncTry
from fons.iter import unique
from fons.log import get_standard_5, quick_logging
from fons.math.round import round_sd
from fons.threads import EliThread
import uxs
from uxs.fintls.basics import calc_price

logger,logger2,tlogger,tloggers,tlogger0 = get_standard_5(__name__)

DEFAULT_LIMITS = {
    'USDT': 100,
    'USD': 100,
    'TUSD': 100,
    'USDC': 100,
    'EUR': 100,
    'BTC': 0.01,
    'ETH': 0.5,
}


class Desk:
    
    include = {
        'automate': False,
        'amount_range': False,
        'random': False,
    }
    
    def __init__(self, xs, symbols, *, amounts={}, include={},
                 safe=False, round_price=None, indexes={}):
        """
        :type xs: uxs.ExchangeSocket
        :param amounts: 
            default (max) amounts by symbol. 
            If an amount of a symbol is not specified, then it is inferred from DEFAULT_LIMITS (~100 USD)
        :param round_price:
            round order price down/up (buy/sell) to sigdigit:
                round_price=3, side='buy', orig_price=8042 -> price=8040
        :param indexes:
            orderbooks to be watched and displayed (but not traded) by symbol, to spot arbitrage etc
            {symbol: [exchange/xs, index_symbol], symbol_2: index_symbol_2_of_the_same_exchange}
        """
        self.xs = xs
        self.symbols = symbols[:]
        self.include = dict(Desk.include, **include)
        self.amounts = dict(amounts)
        self.safe = safe
        self.round_price = round_price
        self.indexes = dict(indexes)
        
        self.exchange = self.xs.exchange
        self.name = '{}Desk'.format(self.exchange)
        self.cur_symbol = None
        self.base = None
        self.quote = None
        self.settle = None
        self.amount_ranges = defaultdict(lambda: dict.fromkeys(['min','max'], (None, None)))
        self.order_deviation = '0.5%'
        # In minutes
        self.automate_value = {'min': 0, 'max': 5}
        self.auto_on = False
        self.queues = {'buySell': asyncio.Queue(loop=self.xs.loop)}
        self.events = {'ok': asyncio.Event(loop=self.xs.loop)}
        self.has_tickers_all = {}
        self.has_ob_merge = {}
        self._symbols = defaultdict(list)
        
        for symbol, amount in self.amounts.items():
            self.amount_ranges[symbol]['max'] = Gui.parse_amount(amount)
        
        self.streamers = {
            self.exchange: self.xs,
        }
        self._init_streamers()
        
        self.gui = Gui(self)
        self.trader = Trader(self)
    
    
    def start(self):
        return call_via_loop_afut(self.run, loop=self.xs.loop)
    
    
    async def run(self, *, start_gui=True):
        if start_gui:
            self.gui.start()
        await asyncio.sleep(3)
        #ok = self.events['ok']
        queue = self.queues['buySell']
        waited = 0
        # Just to activate the account
        #await create_cancel_small_order()
        await safeAsyncTry(self.xs.fetch_balance)
        
        while True:
            if not self.auto_on:
                waited = 0
                wait_time = None
            else:
                wait_min = self.automate_value['min'] * 60
                wait_max = self.automate_value['max'] * 60
                wait_time = max(0, wait_min + (wait_max - wait_min) * random.random() - waited)
            #await ok.wait()
            try: 
                direction = await asyncio.wait_for(queue.get(), wait_time)
            except asyncio.TimeoutError:
                direction = random.randint(0, 1)
            
            while not queue.empty():
                direction = queue.get_nowait()
            
            if isinstance(direction, str):
                # just continue to next cycle
                continue
            
            print('{} - received: {}'.format(self.name, direction))
            await self.wait_till_active()
            r = None
            try:
                r = await self.trader.place_order(direction, self.safe)
            except Exception as e:
                logger2.exception(e)
            
            if r is None:
                continue
            
            await asyncio.sleep(2)
            try:
                order = self.xs.orders[r['id']]
                if order['remaining']:
                    await self.xs.cancel_order(r['id'], r['symbol'])
            except KeyError:
                logger2.error('Unknown order: {}'.format(r['id']))
            except Exception as e:
                logger2.exception(e)
    
    
    def buy(self):
        self.relay(1)
    
    
    def random(self):
        rInt = random.randint(0,1)
        self.relay(rInt)
    
    
    def sell(self):
        self.relay(0)
    
    
    def automate(self):
        self.gui.automate()
    
    
    def relay(self, item, queue='buySell'):
        q = self.queues[queue]
        self.xs.loop.call_soon_threadsafe(functools.partial(q.put_nowait, item))
    
    
    async def wait_till_active(self):
        if self.has_tickers_all[self.exchange]:
            first_p = {'_': 'all_tickers'}
        else:
            first_p = {'_': 'orderbook', 'symbol': self.cur_symbol}
        a_p = {'_': 'account'}
        subs = [first_p, a_p]
        _printed = {}
        i = c = 0
        while i < 2:
            s = subs[i]
            try:
                await self.xs.wait_till_subscription_active(s, timeout=2)
            except asyncio.TimeoutError:
                if not _printed.get(i):
                    logger2.info('{} wait taking longer than usual.'.format(s))
                    _printed[i] = True
                if s['_'] == 'account' and c > 5:
                    i += 1
                    print('Canceling {} wait'.format(s))
            else:
                if _printed.get(i):
                    logger2.info('Wait completed.')
                i += 1
            c += 1
    
    
    def _init_streamers(self):
        self._symbols[self.exchange] = self.symbols[:]
        for symbol, item in self.indexes.items():
            if isinstance(item, str):
                item = [self.exchange, item]
            index_exchange, index_symbol = item
            if not index_exchange:
                index_exchange = self.exchange
            if isinstance(index_exchange, uxs.ExchangeSocket):
                xs, index_exchange = index_exchange, index_exchange.exchange
            else:
                if index_exchange not in self.streamers:
                    self.streamers[index_exchange] = uxs.get_socket(index_exchange, {'loop': self.xs.loop})
                xs = self.streamers[index_exchange]
            if index_exchange not in self.streamers:
                self.streamers[index_exchange] = xs
            if index_symbol not in self._symbols[index_exchange]:
                self._symbols[index_exchange].append(index_symbol)
            self.indexes[symbol] = [index_exchange, index_symbol]
        for exchange, xs in self.streamers.items():
            self.has_tickers_all[exchange] = xs.has_got('all_tickers', ['bid','ask'])
            self.has_ob_merge[exchange] = xs.has_merge_option('orderbook')
        for xs in self.streamers.values():
            xs.ob['sends_bidAsk'] = True
    
    
    @staticmethod
    def _create_subscriptions(self, exchange, to_account=False):
        symbols = self._symbols[exchange]
        if not symbols:
            return
        xs = self.streamers[exchange]
        if self.has_tickers_all[exchange]:
            xs.subscribe_to_all_tickers()
        elif self.has_ob_merge[exchange]:
            xs.subscribe_to_orderbook(symbols)
        else:
            for symbol in symbols:
                xs.subscribe_to_orderbook(symbol)
        if to_account:
            params = {} if 'symbol' not in xs.get_value('account', 'required')\
                      else {'symbol': symbols}
            xs.subscribe_to_account(params)


class Gui:
    def __init__(self, desk):
        """
        :type desk: Desk
        """
        self.desk = desk
        self.prices_updated_ts = defaultdict(float)
        self.thread = None
        self.started = False
    
    
    def start(self):
        if not self.started:
            self.thread = EliThread(target=self.run, name='{}Gui[Thread]'.format(self.desk.name), daemon=True)
            self.thread.start()
            self.started = True
    
    
    def run(self, root=None, pos=0, start_loop=True):
        # Tkinter is tricky when not run in the main thread.
        # Some problems may also arise due to it not being IMPORTED in a different thread
        # from that which it runs on?
        import tkinter as tk
        self.started = True
        if root is None:
            root = tk.Tk()
            root.title('`{}` Desk'.format(self.xs.exchange.upper()))
        self.root = root
        self.pos = pos
        self.desk.cur_symbol = self.desk.symbols[0]
        self.set_currencies()
        self._cur_symbol = tk.StringVar()
        self._cur_symbol.set(self.desk.cur_symbol)
        self._amount_min = tk.StringVar(value='')
        self._amount_max = tk.StringVar(value='')
        self._automate_value = tk.StringVar(value='5')
        self._order_deviation = tk.StringVar(value=self.desk.order_deviation)
        create_order_frame =  tk.Frame(root)
        amount_frame = tk.Frame(root)
        automate_frame = tk.Frame(root)
        sides = {
            'long': {'side': tk.LEFT, 'anchor': tk.W},
            'short': {'side': tk.LEFT, 'anchor': tk.W},
            'automate': {'side': tk.LEFT, 'anchor': tk.W},
            'automate_value': {'side': tk.LEFT, 'anchor': tk.W},
            'amount_min': {'side': tk.LEFT, 'anchor': tk.W},
            'amount_max': {'side': tk.LEFT, 'anchor': tk.W},
        }
        index_text = self.get_index_text()
        self.objects = OrderedDict([
            ['index_label', tk.Label(root, text=index_text)], 
            ['exchange_label', tk.Label(root, text=self.desk.exchange.upper(), fg='red')], 
            ['select_symbol', tk.OptionMenu(root, self._cur_symbol, *self.desk.symbols)],
            ['ok', tk.Button(root, text='OK', command=self.ok)],
            ['index_prices', tk.Label(root, text='---' if index_text else '', fg='red')],
            ['prices', tk.Label(root, text='---', fg='blue')],
            ['create_order_frame', create_order_frame],
            ['long', tk.Button(create_order_frame, text='LONG', command=self.desk.buy, fg='green')],
            ['short', tk.Button(create_order_frame, text='SHORT', command=self.desk.sell, fg='red')],
            ['balances', tk.Label(root, text='---')],
            ['automate_frame', automate_frame],
            ['automate', tk.Button(automate_frame, text='AUTO: off', command=self.desk.automate)],
            ['automate_value', tk.Entry(automate_frame, textvariable=self._automate_value, width=7)],
            ['order_deviation', tk.OptionMenu(root, self._order_deviation, *['0%','0.5%','1%','2%','market'])],
            ['amount_frame', amount_frame],
            ['amount_min', tk.Entry(amount_frame, textvariable=self._amount_min, width=10)],
            ['amount_max', tk.Entry(amount_frame, textvariable=self._amount_max, width=10)],
            ['random', tk.Button(root, text='RANDOM', command=self.desk.random)],
            ['enter', tk.Button(root, text='ENTER', command=self.enter)],
        ])
        _map = {
            'amount_min': 'amount_range',
            'automate_frame': 'automate',
            'random': 'random', 
        }
        coords = {
            'index_label': (0, 0), 
            'exchange_label': (1, 0), 
            'select_symbol': (2, 0),
            'ok': (3, 0),
            'index_prices': (0, 1),
            'prices': (1, 1),
            'create_order_frame': (2, 1),
            'balances': (3, 1),
            'order_deviation': (1, 2),
            'automate_frame': (2, 2),
            'amount_frame': (3, 2),
            'random': (0, 3),
            'enter': (3, 3),
        }
        add = pos * 4
        for name, obj in self.objects.items():
            if name not in _map or self.desk.include[_map[name]]:
                if name in coords:
                    i, j = coords[name]
                    obj.grid(row=add+i, column=j)
                else:
                    info = sides.get(name, {})
                    obj.pack(**info)
        
        self._display_range()
        self.config('select_symbol', fg='red')
        
        for exchange, xs in self.desk.streamers.items():
            to_account = (self.desk.exchange==exchange)
            self.desk._create_subscriptions(self.desk, exchange, to_account)
            self._add_callbacks(exchange)
            xs.start()
        
        self.refresh(300)
        if start_loop:
            self.root.mainloop()
    
    
    def set_currencies(self):
        settle = None
        try:
            market = self.xs.markets[self.desk.cur_symbol]
            base, quote = market['base'], market['quote']
            if market.get('type') not in ('spot', None):
                settle = market.get('settle')
        except KeyError:
            base, quote = self.desk.cur_symbol.split('/')
        
        self.desk.base, self.desk.quote, self.desk.settle = base, quote, settle
    
    
    def get_index_text(self):
        index_exchange, index_symbol = self.desk.indexes.get(self.desk.cur_symbol, [None, None])
        if not index_symbol:
            return ''
        return '{} {}'.format(index_exchange, index_symbol)
    
    
    def refresh(self, recursive=False):
        if self.desk.cur_symbol != self._cur_symbol.get():
            self.desk.cur_symbol = self._cur_symbol.get()
            if hasattr(self.xs, 'go_to_market'):
                self.xs.loop.call_soon_threadsafe(
                    functools.partial(self.xs.go_to_market, self.desk.cur_symbol))
            self.set_currencies()
            index_text = self.get_index_text()
            self.config('index_label', text=index_text)
            self.config('index_prices', text='---' if index_text else '')
            self.update_prices([{'symbol': self.desk.cur_symbol}], True)
            self.update_balances({})
            self._display_range()
        else:
            self._check_range_changes()
            self._check_automate_value_changes()
        
        self._check_subscriptions()
        self.desk.order_deviation = self._order_deviation.get()
        
        if recursive:
            self.root.after(recursive, self.refresh, recursive)
    
    
    def _display_range(self):
        for x in ('min','max'):
            value, cy = self.desk.amount_ranges[self.desk.cur_symbol][x]
            if value is None:
                value = ''
            str_value = str(value)
            if cy is not None:
                str_value += ' '+cy
            var = getattr(self,'_amount_'+x)
            var.set(str_value)
            box = self.objects['amount_'+x]
            box.config(bg='white')
    
    
    def enter(self):
        ok, values = self._check_range_changes(True)
        if ok:
            self.desk.amount_ranges[self.desk.cur_symbol]['min'] = values['min']
            self.desk.amount_ranges[self.desk.cur_symbol]['max'] = values['max']
            self._display_range()
        ok, values = self._check_automate_value_changes(True)
        if ok:
            if self.desk.automate_value != values:
                self.desk.automate_value.update(values)
                self.desk.relay('')
    
    
    @staticmethod
    def parse_amount(value):
        """:returns: (amount, currency)"""
        if value is None:
            return (None, None)
        if isinstance(value, (int, float)):
            return (value, None)
        amount = None
        cy = None
        str_value = value.strip()
        if str_value != '':
            split = str_value.split(' ')
            if len(split) > 1:
                str_value, cy = [_.strip() for _ in split if _.strip()]
            amount = float(str_value)
        if amount is not None and amount < 0:
            raise ValueError(value)
        
        return (amount, cy)
    
    
    def _check_range_changes(self, enter_pressed=False):
        success = True
        values = {}
        for x in ('min','max'):
            var = getattr(self, '_amount_'+x)
            box = self.objects['amount_'+x]
            try:
                amount, cy = self.parse_amount(var.get())
            except ValueError:
                box.config(bg='red')
                success = False
            else:
                values[x] = (amount, cy)
                color = 'white'
                if not enter_pressed and self.desk.amount_ranges[self.desk.cur_symbol][x] != values[x]:
                    # value is correct but unsaved
                    color = 'green'
                box.config(bg=color)
        
        return success, values
    
    
    def _check_automate_value_changes(self, enter_pressed=False):
        success = True
        values = {}
        var = getattr(self, '_automate_value')
        str_value = var.get().strip()
        box = self.objects['automate_value']
        try:
            
            split = [_.strip() for _ in str_value.split('-') if _.strip()]
            if len(split)==1:
                _min = 0
                _max = float(split[0])
            elif len(split)==2:
                _min = float(split[0])
                _max = float(split[1])
            else:
                raise ValueError(str_value)
            if _min > _max or _max==0:
                raise ValueError(str_value)
        except ValueError:
            box.config(bg='red')
            success = False
        else:
            values.update({'min': _min, 'max': _max})
            color = 'white'
            if not enter_pressed and self.desk.automate_value != values:
                # value is correct but unsaved
                color = 'green'
            box.config(bg=color)
        
        return success, values
    
    
    def _check_subscriptions(self):
        targets = [[self.desk.exchange, self.desk.cur_symbol, 'prices']]
        index_exchange, index_symbol = self.desk.indexes.get(self.desk.cur_symbol, [None, None])
        if index_symbol:
            targets += [[index_exchange, index_symbol, 'index_prices']]
        for exchange, symbol, name in targets:
            if self.desk.has_tickers_all[exchange]:
                params = {'_': 'all_tickers'}
            else:
                params = {'_': 'orderbook', 'symbol': symbol}
            xs = self.desk.streamers[exchange]
            if not xs.is_subscribed_to(params, True):
                self.config(name, text='---')
        if not self.desk.xs.is_subscribed_to({'_': 'account'}, True):
            self.config('balances', text='---')
    
    
    def config(self, obj, *args, **kw):
        method = 'config'
        if isinstance(obj, tuple):
            obj, method = obj
        if isinstance(obj, str):
            obj = self.objects[obj]
        self.root.after(1, lambda: getattr(obj, method)(*args, **kw))
    
    
    def ok(self):
        e = self.desk.events['ok']
        self.xs.loop.call_soon_threadsafe(e.set)
        time.sleep(0.1)
        self.xs.loop.call_soon_threadsafe(e.clear)
    
        
    def automate(self):
        if not self.desk.auto_on:
            self.config('automate', text='AUTO: on', bg='green')
        else:
            self.config('automate', text='AUTO: off', bg='white')
        self.desk.auto_on = not self.desk.auto_on
        self.desk.relay('')
    
    
    def update_prices(self, d, force=True):
        targets = [[self.desk.exchange, self.desk.cur_symbol, 'prices']]
        index_exchange, index_symbol = self.desk.indexes.get(self.desk.cur_symbol, [None, None])
        if index_symbol:
            targets += [[index_exchange, index_symbol, 'index_prices']]
        
        for exchange, symbol, name in targets:
            xs = self.desk.streamers[exchange]
            is_ob = not self.desk.has_tickers_all[exchange]
            if is_ob and not force and not any(x['symbol']==symbol for x in d):
                continue
            which = xs.orderbooks if is_ob else xs.tickers
            if symbol not in which:
                self.config(name, text='---')
                continue
            if is_ob:
                ob = xs.orderbooks[symbol]
                bid, ask = ob['bids'][0][0], ob['asks'][0][0]
            else:
                t = xs.tickers[symbol]
                bid, ask = t['bid'], t['ask']
            text = '{} | {}'.format(round(bid, 2), round(ask, 2))
            
            if time.time() - self.prices_updated_ts[(exchange, symbol)] > 10 or force:
                self.config(name, text=text)
                self.prices_updated_ts[(exchange, symbol)] = time.time()
    
    
    def update_balances(self, d):
        _round = {'BTC': 3}
        bals = []
        cys = [self.desk.quote]
        cys += [self.desk.base] if not self.desk.settle else [self.desk.settle]
        for cy in cys:
            if cy in self.xs.balances:
                bals.append(round(self.xs.balances[cy]['free'], _round.get(cy, 2)))
            else:
                bals.append('--')
        text = '{} {} | {} {}'.format(cys[0], *bals, cys[1])
        self.config('balances', text=text)
    
    
    def _add_callbacks(self, exchange):
        xs = self.desk.streamers[exchange]
        if self.desk.has_tickers_all[exchange]:
            xs.add_callback(self.update_prices, 'ticker', -1)
        else:
            for symbol in self.desk._symbols[exchange]:
                xs.add_callback(self.update_prices, 'orderbook', symbol)
        if exchange == self.desk.exchange:
            xs.add_callback(self.update_balances, 'balance', -1)
    
    @property
    def xs(self):
        return self.desk.xs


class Trader:
    def __init__(self, desk):
        """:type desk: Desk"""
        self.desk = desk
    
    
    async def create_cancel_small_order(self):
        amount = None
        price = None
        r = await safeAsyncTry(self.xs.create_order,(self.desk.cur_symbol,'limit','buy',amount,price))
        if r is not None:
            await safeAsyncTry(self.xs.cancel_order,(r['id'],r['symbol']))
    
    
    def _get_prices(self):
        return {symbol: t['last'] if t.get('last') else (t['bid'] + t['ask']) / 2
                for symbol,t in self.xs.tickers.items() if t.get('last') or t.get('bid')}
    
    
    def _get_min_amount(self, symbol):
        min_amount = self.xs.markets[symbol]['limits']['amount']['min']
        if min_amount is None:
            min_amount = self.xs.markets[symbol]['precision']['amount']
            if self.xs.api.precisionMode != TICK_SIZE:
                min_amount = 10 ** -min_amount
        return min_amount
    
    
    def adjust_user_limits(self, symbol, user_limits):
        base, quote = symbol.split('/')
        min_amount = self._get_min_amount(symbol)
        _min, _min_cy = user_limits['min']
        _max, _max_cy = user_limits['max']
        is_literal = {
            'min': _min is not None and _min_cy is None,
            'max': _max is not None and _max_cy is None,
        }
        lotSize = self.xs.markets[symbol]['lotSize']
        type = self.xs.markets[symbol].get('type')
        tlogger.debug('{} initial limits: {}'.format(symbol, user_limits))
        
        def _calc_cy_amount(cy_amount, cy):
            if cy == base:
                return cy_amount
            return cy_amount / calc_price((base, cy), self._get_prices(), self.xs.api.load_cy_graph(), max_len=3)
        
        if _min_cy is not None:
            _min = _calc_cy_amount(_min, _min_cy)
        
        if _min is None:
            _min = min_amount
            is_literal['min'] = True
        
        if _max is None:
            for cy in DEFAULT_LIMITS:
                if any(m['quote']==cy for m in self.xs.markets.values()):
                    try:
                        _max = _calc_cy_amount(DEFAULT_LIMITS[cy], cy)
                    except RuntimeError:
                        continue
                    break
            if _max is None:
                raise RuntimeError('Could not calculate max amount from any DEFAULT_LIMITS')
        elif _max_cy is not None:
            _max = _calc_cy_amount(_max, _max_cy)
        
        final = {'min': _min, 'max': _max}
        for x in ('min', 'max'):
            if not is_literal[x]:
                if type == 'swap':
                    final[x] *= self._get_prices()[symbol]
                final[x] /= lotSize
        
        if final['min'] > final['max']:
            raise ValueError('min > max: {} > {} | {}'.format(final['min'], final['max'], user_limits))
        
        tlogger.debug('{} final limits: {}'.format(symbol, final))
        
        return final
    
    
    def verify_amount(self, symbol, side, amount, price):
        min_cost = self.xs.markets[symbol]['limits'].get('cost', {}).get('min')
        min_amount = self._get_min_amount(symbol)
        cost = amount * price
        logger2.info('Placing {} order: p: {} a: {}'.format(side.upper(), price, amount))
        if amount < min_amount:
            logger.info('Order amount is below minimum: {} < {}.'.format(amount, min_amount))
            return False
        #elif min_cost is not None and cost < min_cost:
        #    logger.info('Order cost is below minimum: {} < {}'.format(cost, min_cost))
        #    return False
        return True
    
    
    @staticmethod
    def generate_random_amount(_min, _max):
        return _min + max(0, random.random() * (_max - _min))
    
    
    def calc_amount(self, symbol, side, price, user_limits):
        market = self.xs.markets[symbol]
        base, quote = market['base'], market['quote']
        b_quote = self.xs.balances['free'].get(quote)
        b_base = self.xs.balances['free'].get(base)
        _min = user_limits['min']
        _max = user_limits['max']
        lotSize = self.xs.markets[symbol]['lotSize']
        type = self.xs.markets[symbol].get('type')
        
        if type in ('spot', None):
            _min *= lotSize
            _max *= lotSize
            if side == 'buy':
                _min *= price
                _max *= price
                _max = min(b_quote, _max)
                amount_quote = self.generate_random_amount(_min, _max)
                if b_quote < amount_quote:
                    logger.info('Too few {}: {} < {}.'.format(quote, b_quote, amount_quote))
                    return None
                amount = amount_quote / price
            else:
                _max = min(b_base, _max)
                amount = self.generate_random_amount(_min, _max)
                if b_base < amount:
                    logger.info('Too few {}: {} < {}.'.format(base, b_base, amount))
                    return None
            amount /= lotSize
        else:
            amount = self.generate_random_amount(_min, _max)
        
        return amount
    
    
    def calc_price(self, symbol, side, deviation, round_price=None):
        if self.desk.has_tickers_all[self.xs.exchange]:
            tSide = ['bid','ask'][side=='buy']
            bidAsk = self.xs.tickers[symbol][tSide]
        else:
            obSide = ['bids','asks'][side=='buy']
            bidAsk = self.xs.orderbooks[symbol][obSide][0][0]
        sign = (-1)**(side=='sell')
        price = bidAsk * (1 + sign*deviation)
        if round_price is not None:
            method = ['up','down'][side=='buy']
            price2 = round_sd(price, round_price, method, accuracy=2)
            if side=='buy' and price2 < bidAsk or side=='sell' and price2 > bidAsk:
                logger2.error("{} - price {} cannot be rounded to sigdigit {} because the resulting price ({}) "
                              "wouldn't fill into order book".format(symbol, price, round_price, price2))
            else:
                price = price2
        
        return price
    
    
    async def place_order(self, rInt=None, safe=False):
        symbol = self.desk.cur_symbol
        # random order
        if rInt is None:
            rInt = random.randint(0,1)
        side = ['sell','buy'][rInt]
        deviation = self.desk.order_deviation
        type = 'market' if deviation=='market' else 'limit'
        deviation = float(deviation[:-1])/100 if deviation!='market' else 0
        round_price = self.desk.round_price
        price = self.calc_price(symbol, side, deviation, round_price)
        limits = self.adjust_user_limits(symbol, self.desk.amount_ranges[symbol])
        if not self.desk.include['amount_range']:
            limits['min'] = limits['max']
        amount = self.calc_amount(symbol, side, price, limits)
        
        r = None
        if amount is not None and self.verify_amount(symbol, side, amount, price):
            if type == 'market':
                price = None
            args = (symbol, type, side, amount, price)
            #logger2.info('Placing order: {}'.format(args))
            if safe:
                logger2.debug('Press ok to confirm')
                await self.desk.events['ok'].wait()
            r = await self.xs.create_order(*args)
            print(r)
        
        return r
    
    @property
    def xs(self):
        return self.desk.xs


class Table:
    """Join multiple desks together"""
    
    def __init__(self, desks=[]):
        self.desks = []
        self.thread = None
        self.started = False
        self.loop = None
        
        self.streamers = {}
        self.trade_exchanges = []
        self.has_tickers_all = {}
        self.has_ob_merge = {}
        self._symbols = defaultdict(list)
        
        for desk in desks:
            self.add_desk(desk)
    
    
    def add_desk(self, desk):
        if self.started:
            raise RuntimeError("Desk cannot be added when Table is running")
        
        if self.loop is None:
            self.loop = desk.xs.loop
        elif self.loop != desk.xs.loop:
            raise ValueError("Streamers' event loops don't match")
        
        self.streamers[desk.exchange] = desk.xs
        if desk.exchange not in self.trade_exchanges:
            self.trade_exchanges.append(desk.exchange)
        
        self.has_tickers_all.update(desk.has_tickers_all)
        self.has_ob_merge.update(desk.has_ob_merge)
        
        for exchange, symbols in desk._symbols.items():
            self._symbols[exchange] = list(unique(self._symbols[exchange] + symbols))
            if exchange not in self.streamers:
                self.streamers[exchange] = desk.streamers[exchange]
                
        self.desks.append(desk)
    
    
    @classmethod
    def from_args(cls, data):
        streamers =  {}
        data2 = []
        for exchange, symbols, indexes, kw in data:
            if isinstance(exchange, uxs.ExchangeSocket):
                streamers[exchange.exchange] = xs = exchange
                exchange = exchange.exchange
            else:
                exchange, id = uxs._interpret_exchange(exchange)
                if not id:
                    id = 'TRADE'
                if exchange not in streamers:
                    test = kw.get('test', False)
                    streamers[exchange] = uxs.get_socket(exchange, {'auth': id, 'test': test})
                xs = streamers[exchange]
            data2.append([xs, symbols, indexes, kw])
        
        data3 = []
        for xs, symbols, indexes, kw in data2:
            indexes2 = {}
            for symbol, index in indexes.items():
                index_exchange, index_symbol = index
                index_exchange, id = uxs._interpret_exchange(index_exchange)
                if index_exchange not in streamers:
                    test = kw.get('test', False)
                    streamers[index_exchange] = uxs.get_socket(index_exchange, {'auth': id, 'test': test})
                indexes2[symbol] = (streamers[index_exchange], index_symbol)
            kw2 = {k:v for k,v in kw.items() if k!='test'}
            data3.append([xs, symbols, indexes2, kw2])
        
        print(data3)
        desks = []
        for xs, symbols, indexes, kw in data3:
            desks.append(Desk(xs, symbols, indexes=indexes, **kw))
        
        return cls(desks)
    
    
    def start(self):
        return call_via_loop_afut(self.run, loop=self.loop)
    
    
    async def run(self):
        self.started = True
        self.thread = EliThread(target=self.run_gui, name='TableGui[Thread]', daemon=True)
        self.thread.start()
        await asyncio.sleep(0.5)
        await asyncio.wait([desk.run(start_gui=False) for desk in self.desks])
    
    
    def run_gui(self):
        import tkinter as tk
        self.root = tk.Tk()
        exchanges = [desk.exchange.upper() for desk in self.desks]
        self.root.title('`{}` Desk'.format(', '.join(exchanges)))
        for exchange in self.streamers:
            to_account = (exchange in self.trade_exchanges)
            Desk._create_subscriptions(self, exchange, to_account)
        # build the frames, add callbacks
        for i, desk in enumerate(self.desks):
            desk.gui.run(self.root, pos=i, start_loop=False)
        self.root.mainloop()


def parse_keywords(group):
    apply = {}
    apply['include'] = lambda x: x.split(',')
    apply['test_loggers'] = int
    apply['round_price'] = int
    apply['amounts'] = lambda x: dict([y.split('=') for y in x.split(',')])
    
    return parse_argv(group, apply)


def parse_group(group):
    exchange = group[0]
    symbols = []
    indexes = {}
    special = ['test','safe']
    i = 1
    while i < len(group):
        item = group[i]
        if item.startswith('-') or '=' in item or item in special:
            break
        loc1, loc2 = item.find('{'), item.find('}')
        if [loc1, loc2].count(-1) == 1 or loc1 > loc2:
            raise ValueError(item)
        symbol = item[:loc1] if loc1 != -1 else item
        if not symbol:
            raise ValueError(item)
        index_symbol = None
        index_exchange = None
        if loc1 != -1:
            spl = item[loc1+1:loc2].split(',')
            if len(spl) == 1:
                index_exchange = spl[0]
            elif len(spl) == 2:
                index_exchange, index_symbol = spl
            else:
                raise ValueError(item)
        if index_exchange and not index_symbol:
            index_symbol = symbol
        elif index_symbol and not index_exchange:
            index_exchange = exchange
        symbol = symbol.upper()
        if index_symbol:
            index_symbol = index_symbol.upper()
        symbols.append(symbol)
        if index_symbol:
            indexes[symbol] = (index_exchange, index_symbol)
        i += 1
    if not symbols:
        raise ValueError('Group contains no symbols: {}'.format(group))
    p = parse_keywords(group[i:])
    
    return exchange, symbols, indexes, p


async def run(argv=sys.argv, conn=None):
    final_args = []
    argv_orig = argv[:]
    argv = argv[1:]
    argv_defaults = []
    if '----' in argv:
        loc = argv.index('----')
        argv_defaults = argv[loc+1:]
        argv = argv[:loc]
    
    defaults = parse_keywords(argv_defaults)
    
    while argv:
        loc = len(argv)
        if '--' in argv:
            loc = argv.index('--')
        group, argv = argv[:loc], argv[loc+1:]
        exchange, symbols, indexes, p = parse_group(group)
        
        safe = p.contains('safe') or defaults.contains('safe')
        include = dict(dict.fromkeys(defaults.get('include',[]), True),
                       **dict.fromkeys(p.get('include',[]), True))
        amounts = dict(defaults.get('amounts', {}),
                       **p.get('amounts', {}))
        round_price = p.get('round_price', defaults.get('round_price', None))
        test = p.contains('test') or defaults.contains('test')
        kw = {
            'safe': safe,
            'include': include,
            'amounts': amounts,
            'round_price': round_price,
            'test': test,
        }
        final_args.append([exchange, symbols, indexes, kw])
    
    test_loggers = defaults.get('test_loggers', 2)
    quick_logging(test_loggers)
    
    table = Table.from_args(final_args)
    await table.start()


def main(argv=sys.argv, conn=None):
    asyncio.get_event_loop().run_until_complete(run(argv, conn))


if __name__ == '__main__':
    main()
