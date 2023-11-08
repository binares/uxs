"""
Let the system automatically place stop orders while trading
"""
import sys
import asyncio

from collections import defaultdict
import functools
import tkinter as tk
import tkinter.scrolledtext as scrolledtext

from uxs.ui._queue import SymbolActionQueue
import uxs
from fons.aio import call_via_loop_afut
from fons.argv import parse_argv, is_positional
from fons.debug import safeAsyncTry
from fons.dict_ops import deep_update
from fons.event import empty_queue
from fons.log import get_standard_5, quick_logging
from fons.threads import EliThread

logger, logger2, tlogger, tloggers, tlogger0 = get_standard_5(__name__)


class StopLosser:
    stop_point = 0.8
    stop_point_range = (0.1, 0.95)
    # the change between current_stop_order_price & new_stop_order_price
    # within the range of (liq_price, entry_price)
    edit_worthy_change = 0.03
    queue_maxsize = None
    start_xs = True
    confirm = False
    symbols = None

    def __init__(self, xs, config={}):
        """:type xs: uxs.ExchangeSocket"""
        for attr, new in config.items():
            prev = getattr(self, attr, None)
            new = deep_update(prev, new, copy=True)
            setattr(self, attr, new)

        if getattr(self, "stop_points", None) is None:
            self.stop_points = {}

        self.xs = xs
        self.loop = self.xs.loop
        self.queues = defaultdict(lambda: SymbolActionQueue(self.queue_maxsize))
        self.futures = {}
        self.queries = defaultdict(lambda: {"data": {}, "answer": None})
        self.closed = asyncio.Event()
        self.gui = Gui(self)

        self.stop_point_range = self.gui.verify_stop_point_range(self.stop_point_range)
        self.stop_point = self.gui.verify_stop_point(self.stop_point, False)

        for symbol, stop_point in self.stop_points.items():
            self.stop_points[symbol] = self.gui.verify_stop_point(stop_point)

        self._stopped = asyncio.Event(loop=self.loop)
        self._stopped.set()

    def on_position_update(self, updates):
        for update in updates:
            symbol = update["symbol"]
            changes = update["data"]
            self.manage(symbol, changes)

    def on_order_update(self, updates):
        for update in updates:
            symbol = update["symbol"]
            self.manage(symbol)

    async def manage_cycler(self):
        while not self._stopped.is_set():
            for s in self.gui.symbol_rows:
                self.manage(s)
            try:
                await asyncio.wait_for(self._stopped.wait(), 10)
            except asyncio.TimeoutError:
                pass

    def manage(self, symbol, changes=None, force_cancel=False):
        if self.symbols is not None and symbol not in self.symbols:
            # previously this was due to technical limitations, but now keep it this way
            # to not place unintended stop-loss orders
            return

        if symbol not in self.gui.symbol_rows:
            self.xs.subscribe_to_own_market(symbol)
            self.gui.add_symbol(symbol)

        if not self.xs.positions.get(symbol, {}).get("amount"):
            self.gui.empty_query(symbol)

        if changes is not None and not any(
            x in changes for x in ["amount", "price", "liq_price"]
        ):
            return

        is_account_active = self.xs.is_subscribed_to({"_": "account"}, True)
        is_own_market_active = self.xs.is_subscribed_to(
            {"_": "own_market", "symbol": symbol}, True
        )

        if not is_account_active or not is_own_market_active:
            return

        enabled = self.gui.enabled_vars[self.gui.symbol_rows[symbol]].get()

        side, amount, stop_price, is_edit_worthy = self.calc_order_params(symbol)
        o = self.get_stop_order(symbol)
        q = self.queues[symbol]
        wait_on_cancel = None

        if o is not None:
            cancel = not amount or side != o["side"]
            if (cancel or force_cancel) and not q.contains("cancel"):
                f = call_via_loop_afut(
                    self.cancel, (symbol,), loop=self.loop, cb_loop=self.loop
                )
                q.put(f, "cancel")
                wait_on_cancel = q.get("cancel")

            elif (
                enabled
                and amount
                and not q.contains("edit")
                and not q.contains("create")
                and is_edit_worthy
            ):
                f2 = call_via_loop_afut(
                    self.place,
                    (symbol, side, amount, stop_price, "edit", wait_on_cancel),
                    loop=self.loop,
                    cb_loop=self.loop,
                )
                q.put(f2, "edit")

        if (
            enabled
            and amount
            and not q.contains("create")
            and not q.contains("edit")
            and (o is None and not q.contains("cancel") or wait_on_cancel is not None)
        ):
            f3 = call_via_loop_afut(
                self.place,
                (symbol, side, amount, stop_price, "create", wait_on_cancel),
                loop=self.loop,
                cb_loop=self.loop,
            )
            q.put(f3, "create")

    def calc_order_params(self, symbol):
        p = self.xs.positions[symbol]
        amount = p["amount"]
        liq_price = p["liq_price"]

        if not amount or not liq_price:
            return None, None, None, None

        stop_point = self.stop_points.get(symbol)
        if stop_point is None:
            stop_point = self.stop_point

        side = "buy" if amount < 0 else "sell"
        entry_price = p["price"]
        liq_price = p["liq_price"]
        diff = abs(liq_price - entry_price)
        mp = stop_point if side == "buy" else 1 - stop_point
        stop_price = min(liq_price, entry_price) + diff * mp
        stop_price = self.xs.api.round_price(symbol, stop_price, side)

        is_edit_worthy = True
        o = self.get_stop_order(symbol)
        if o is not None:
            # if stop order is already placed, check if the position params
            # have "changed enough" for the existing order to be edit worthy
            cur_stop_price = o["stop"]
            if (
                o["side"] != side
                or (
                    side == "buy"
                    and cur_stop_price > liq_price
                    or side == "sell"
                    and cur_stop_price < liq_price
                )
                or o["amount"] != abs(amount)
            ):
                is_edit_worthy = True
            else:
                change = abs(stop_price - cur_stop_price) / diff
                is_edit_worthy = change > self.edit_worthy_change

        return side, abs(amount), stop_price, is_edit_worthy

    async def place(self, symbol, side, amount, price, action="create", wait_on=None):
        if wait_on is not None:
            await wait_on

        d = {
            "stop": price,
            "amount": amount,
            "side": side,
            "positionQty": self.xs.positions.get(symbol, {}).get("amount"),
        }
        query_matches = self.queries[symbol]["data"] == d
        query_answer = self.queries[symbol]["answer"]
        if query_matches and query_answer == "NO":
            # The query has already been answered before
            return

        verb = action[: -1 if action.endswith("e") else None] + "ing"
        order_str = "stop order: {} {} {} {} (stopPx)".format(
            symbol, side, amount, price
        )

        if self.confirm and query_answer is None:
            question = "{} {}?".format(action.capitalize(), order_str)
            query_answer = await self.gui.query(question)
            self.queries[symbol] = {"data": d, "answer": query_answer}

        args = []
        o = self.get_stop_order(symbol)
        if o is not None and action == "edit":
            args.append(o["id"])

        if query_answer in ("YES", None):
            logger.debug("{} {}".format(verb.capitalize(), order_str))
            method = getattr(self.xs, "{}_order".format(action))
            await method(*args, symbol, "stop", side, amount, None, {"stopPx": price})

    async def cancel(self, symbol, wait_on=None):
        if wait_on is not None:
            try:
                await wait_on
            except Exception:
                pass
        o = self.get_stop_order(symbol)
        if o is not None:
            logger.debug("{} - canceling stop order".format(symbol))
            await self.xs.cancel_order(o["id"])
            self.gui.empty_query(symbol)
        else:
            logger.debug("{} - nothing to cancel".format(symbol))

    def get_stop_order(self, symbol):
        return next(
            (
                x
                for x in self.xs.open_orders.values()
                if x["symbol"] == symbol and x.get("stop")
            ),
            None,
        )

    def start(self):
        self._stopped.clear()
        self.xs.add_callback(self.on_position_update, "position", -1)
        self.xs.add_callback(self.on_order_update, "order", -1)
        if self.start_xs:
            self.xs.start()
        self.gui.start()
        self.futures["manage_cycler"] = asyncio.ensure_future(
            safeAsyncTry(self.manage_cycler)
        )

    def stop(self):
        self._stopped.set()
        self.xs.remove_callback(self.on_position_update, "position", -1)
        self.xs.remove_callback(self.on_order_update, "order", -1)

    def close(self):
        self.stop()

        async def _close():
            await self.xs.stop()
            self.xs.close()
            self.closed.set()

        call_via_loop_afut(_close, loop=self.loop)


class Gui:
    def __init__(self, sl):
        """:type sl: StopLosser"""
        self.sl = sl
        self.events = {
            "pause": asyncio.Event(),
        }
        self._started = False
        self.yes_no_queue = asyncio.Queue(loop=self.sl.loop)
        # self.events['pause'].set()

        self.rows_count = max(4, 6)
        self.center_row = int(self.rows_count / 2)
        self.symbol_rows = {}
        self.row_symbols = {}

    def start(self):
        if not self._started:
            self.thread = EliThread(target=self.run, name="TkThread", daemon=True)
            self.thread.start()
            self._started = True

    def run(self):
        self.root = root = tk.Tk()
        id = self.sl.xs.exchange
        if self.sl.symbols is not None:
            id += ": " + " ".join(self.sl.symbols)
        self.root.title("`{}` stoploss helper".format(id))
        root.geometry("500x300+500+500")

        self.grid = grid = tk.Frame(root)
        grid.pack()  # expand=False)#, fill=tk.BOTH)

        self.lower_frame = lower_frame = tk.Frame(root)  # , height=70, width=300)
        self.yes_no_frame = yes_no_frame = tk.Frame(lower_frame)
        self.text_frame = text_frame = tk.Frame(lower_frame)
        lower_frame.pack()
        yes_no_frame.pack(side=tk.LEFT, anchor=tk.W)
        text_frame.pack(side=tk.LEFT)  # expand=False, fill=tk.X)

        # height and width infer the number of characters
        # self.text_box = tk.Text(text_frame, fg='grey', height=4, width=55, state=tk.DISABLED)
        self.text_box = scrolledtext.ScrolledText(
            text_frame, fg="black", height=4, width=55, state=tk.DISABLED
        )
        # self.text_box2 = tk.Text(text_frame, fg='black', height=4, width=55, state=tk.DISABLED)
        self.text_box2 = scrolledtext.ScrolledText(
            text_frame, fg="black", height=4, width=55, state=tk.DISABLED
        )
        self.text_box.pack()
        self.text_box2.pack()
        self.yes = tk.Button(
            yes_no_frame, text="YES", command=functools.partial(self._put, "YES")
        )
        self.no = tk.Button(
            yes_no_frame, text="NO", command=functools.partial(self._put, "NO")
        )
        self.yes.pack()
        self.no.pack()

        self.exit_var = tk.IntVar()
        self.stop_point_var = tk.StringVar()
        self.stop_point_range_var = tk.StringVar()
        self.stop_point_vars = {i: tk.StringVar() for i in range(self.rows_count)}
        self.enabled_vars = {i: tk.IntVar(value=1) for i in range(self.rows_count)}
        self.buttons = {
            "pause": tk.Button(grid, text="PAUSE", command=self.pause),
            "exit": tk.Button(grid, text="EXIT", command=self.exit),
            "exit_checkbox": tk.Checkbutton(grid, variable=self.exit_var),
            "ok": tk.Button(grid, text="OK", command=self.ok),
            "stop_point": tk.Entry(grid, textvariable=self.stop_point_var, width=10),
            "stop_point_range": tk.Entry(
                grid, textvariable=self.stop_point_range_var, width=10
            ),
        }
        for i, obj in enumerate(self.buttons.values()):
            obj.grid(row=i, column=0)

        def empty_and_manage(i):
            try:
                symbol = self.row_symbols[i]
            except KeyError:
                return
            self.empty_query(symbol)
            self.sl.manage(symbol)

        self.tkobjs = {
            i: {
                "symbol": tk.Label(grid, text="-------"),
                "amount": tk.Label(grid, text="-------"),
                "entry": tk.Label(grid, text="-------"),
                "stop_amount": tk.Label(grid, text="-------"),
                "stop": tk.Label(grid, text="-------"),
                "stop_point": tk.Entry(
                    grid, textvariable=self.stop_point_vars[i], width=6
                ),
                "liquidation": tk.Label(grid, text="-------"),
                "enabled": tk.Checkbutton(grid, variable=self.enabled_vars[i]),
                "cancel": tk.Button(
                    grid,
                    text="cancel",
                    command=functools.partial(self.cancel_stop_order, i),
                ),
                "empty": tk.Button(
                    grid, text="empty", command=functools.partial(empty_and_manage, i)
                ),
            }
            for i in range(self.rows_count)
        }
        self.columns = {
            x: j + 1
            for j, x in enumerate(
                [
                    "symbol",
                    "amount",
                    "entry",
                    "stop_amount",
                    "stop",
                    "liquidation",
                    "stop_point",
                    "enabled",
                    "cancel",
                    "empty",
                ]
            )
        }
        for i in range(self.rows_count):
            for x, j in self.columns.items():
                self.tkobjs[i][x].grid(row=i, column=j)

        self.set_initial_values()
        self.check_changes(recursive=250)
        self.check_row_values(recursive=1000)
        # self.test_display()
        self.root.mainloop()

    def set_initial_values(self):
        self.stop_point_var.set(str(self.sl.stop_point))
        self.stop_point_range_var.set(str(self.sl.stop_point_range))

    def add_symbol(self, symbol):
        i = next((i for i in range(self.rows_count) if i not in self.row_symbols), None)
        if i is None:
            self.add_row()
        self.symbol_rows[symbol] = i
        self.row_symbols[i] = symbol
        self.tkobjs[i]["symbol"].config(text=symbol)
        self.update_row(symbol)

    def update_row(self, symbol):
        if isinstance(symbol, int):
            symbol = self.row_symbols[symbol]
        i = self.symbol_rows[symbol]

        keys = ["amount", "entry", "liquidation", "stop_amount", "stop"]
        null = "-------"
        updates = dict.fromkeys(keys, null)
        updates["symbol"] = symbol

        o = self.sl.get_stop_order(symbol)
        p = self.sl.xs.positions.get(symbol)

        _map = {"amount": "amount", "entry": "price", "liquidation": "liq_price"}
        if p is not None:
            for k, k2 in _map.items():
                v = p.get(k2)
                updates[k] = v if v is not None else null

        _map2 = {"stop_amount": "amount", "stop": "stop"}
        if o is not None:
            for k, k2 in _map2.items():
                v = o.get(k2)
                if k == "stop_amount" and v is not None:
                    v = abs(v) if o["side"] == "buy" else -abs(v)
                updates[k] = v if v is not None else null

        fg = "black" if self.enabled_vars[i].get() else "grey"
        for k, v in updates.items():
            self.tkobjs[i][k].config(text=str(v), fg=fg)

    def ok(self):
        changed = []
        errors = []

        for x in ["stop_point", "stop_point_range"]:
            attr = x + "_var"
            var = getattr(self, attr)
            txt = var.get()
            fh_value = getattr(self.sl, x)
            if txt == "":
                var.set(str(fh_value))
            elif txt != str(fh_value):
                try:
                    new_value = getattr(self, "verify_{}".format(x))(txt)
                except AssertionError:
                    errors.append([x, txt])
                    var.set(str(fh_value))
                else:
                    setattr(self.sl, x, new_value)
                    var.set(str(new_value))
                    if new_value != fh_value:
                        changed.append(x)

        for s, i in self.symbol_rows.items():
            var = self.stop_point_vars[i]
            txt = var.get()
            fh_value = self.sl.stop_points.get(s)
            try:
                if txt == "":
                    new_value = None
                else:
                    new_value = self.verify_stop_point(txt)
            except AssertionError:
                errors.append([(s, "stop_point"), txt])
            else:
                self.sl.stop_points[s] = new_value
                if fh_value != new_value:
                    changed.append((s, "stop_point"))
                var.set(str(new_value) if new_value is not None else "")

        self.error(errors)
        self.on_change(changed)

    def check_changes(self, recursive=None):
        for x in ("stop_point", "stop_point_range"):
            attr = x + "_var"
            color = (
                "green"
                if getattr(self, attr).get() != str(getattr(self.sl, x))
                else "white"
            )
            self.buttons[x].config(background=color)

        symbolless_rows = [
            i for i in range(self.rows_count) if i not in self.row_symbols
        ]

        for i, s in self.row_symbols.items():
            str_fh_value = (
                ""
                if self.sl.stop_points.get(s) is None
                else str(self.sl.stop_points[s])
            )
            color = (
                "green" if self.stop_point_vars[i].get() != str_fh_value else "white"
            )
            self.tkobjs[i]["stop_point"].config(background=color)

        for i in symbolless_rows:
            self.tkobjs[i]["stop_point"].config(background="grey")
            self.stop_point_vars[i].set("")

        if recursive is not None:
            self.root.after(recursive, self.check_changes, recursive)

    def check_row_values(self, recursive=None):
        for symbol in self.symbol_rows:
            self.update_row(symbol)

        if recursive is not None:
            self.root.after(recursive, self.check_row_values, recursive)

    def verify_stop_point(self, x, allow_null=True):
        a, b = self.sl.stop_point_range
        if isinstance(x, str):
            try:
                x = float(x)
            except ValueError:
                raise AssertionError

        assert (x is None and allow_null) or (x is not None and a <= x <= b)
        return x

    def verify_stop_point_range(self, x):
        def _to_range(txt):
            a, b = txt[1:-1].split(",")
            return (float(a), float(b))

        if isinstance(x, str):
            try:
                x = _to_range(x)
            except (IndexError, ValueError):
                raise AssertionError

        assert (
            isinstance(x, tuple)
            and len(x) == 2
            and all(isinstance(y, float) for y in x)
            and 0.001 <= x[0] <= x[1] <= 0.999
        )
        return x

    def change_stop_point(self, i):
        symbol = self.tkobjs[i]["symbol"].text
        prev_stop_point = self.sl.stop_points.get(symbol)
        new_point = float(self.tkobjs[i]["stop_point"].get())
        if new_point == prev_stop_point:
            return
        self.sl.assert_satisfies_stop_range(new_point)

    def on_change(self, changes):
        pass

    def error(self, incorrect_values=[], errors=[]):
        text_rows = []

        for id, value in incorrect_values:
            id_str = str(id) if not isinstance(id, str) else "'{}'".format(id)
            row_txt = "Incorrect {}: {}".format(id_str, value)
            text_rows.append(row_txt)

        for e in errors:
            text_rows.append(repr(e))

        txt = "\n".join(text_rows)
        self.display(txt, clear=True)

    def display(self, txt="", box=1, clear=False):
        text_box = {1: self.text_box, 2: self.text_box2}[box]
        text_box.config(state=tk.NORMAL)
        if clear:
            text_box.delete("1.0", tk.END)
        else:
            txt = "\n" + txt
        logger.debug(txt)
        text_box.insert(tk.END, txt)
        text_box.config(state=tk.DISABLED)
        # Autoscroll to the bottom
        text_box.yview(tk.END)

    def test_display(self):
        for k in range(6):
            clear = not k
            self.root.after(
                200 + k * 10, self.display, "Text box 1; entry: {}".format(k), 1, clear
            )
            self.root.after(
                250 + k * 10, self.display, "Text box 2; entry: {}".format(k), 2, clear
            )
        self.root.after(
            200 + k * 10 + 3000, self.display, "Text box 1; Cleared", 1, True
        )

    def _put(self, yes_or_no):
        call_via_loop_afut(self.yes_no_queue.put, (yes_or_no,), loop=self.sl.loop)

    async def query(self, question):
        self.display("\n" + question, 2)
        empty_queue(self.yes_no_queue)
        answer = await self.yes_no_queue.get()
        self.display(answer, 2)
        return answer

    def empty_query(self, symbol):
        if isinstance(symbol, int):
            try:
                symbol = self.row_symbols[symbol]
            except KeyError:
                return
        try:
            del self.sl.queries[symbol]
        except KeyError:
            pass

    def cancel_stop_order(self, i):
        if i not in self.row_symbols:
            return
        symbol = self.tkobjs[i]["symbol"]["text"]
        order = self.sl.get_stop_order(symbol)
        if order is None:
            return
        self.sl.manage(symbol, force_cancel=True)

    def pause(self):
        e = self.events["pause"]
        b = self.buttons["pause"]
        if e.is_set():
            e.clear()
            self.sl.loop.call_soon_threadsafe(self.sl.start)
            b.config(text="PAUSE")
        else:
            e.set()
            self.sl.loop.call_soon_threadsafe(self.sl.stop)
            b.config(text="-RUN-")

    def exit(self):
        if not self.exit_var.get():
            return
        self.sl.loop.call_soon_threadsafe(self.sl.close)


def main(argv=sys.argv, conn=None):
    argv_orig = argv
    exchange = argv[1]
    argv = argv[2:]
    known = [
        "no-display",
        "no_display",
        "nd",
        "no-xs",
        "no_xs",
        "test",
        "confirm",
        "log",
        "loggers",
    ]
    symbols = []

    while argv and argv[0] not in known and is_positional(argv[0]):
        symbols.append(argv[0].upper())
        argv = argv[1:]

    apply = {"log": int, "loggers": int}
    p = parse_argv(argv, apply)

    unknown = [x for x in p if x not in known]
    if unknown:
        raise ValueError("Got unknown argv: {}".format(unknown))

    nr_test_loggers = p.get("log", p.get("test_loggers", 2))
    quick_logging(nr_test_loggers)

    sl_config = {}
    params = {}
    if symbols:
        params["symbol"] = symbols
        sl_config["symbols"] = symbols

    xs = None
    if isinstance(exchange, uxs.ExchangeSocket):
        xs, exchange = exchange, exchange.exchange

    print("Exchange: {}. symbols: {}".format(exchange, symbols))

    test = p.contains("test")

    config = {"test": test}
    if not p.contains("nd", "no-display", "no_display"):
        config["connection_defaults"] = {"handle": print}

    if xs is None:
        _exchange = exchange + ":TRADE" if ":" not in exchange else exchange
        xs = uxs.get_streamer(_exchange, config)

    xs.subscribe_to_account()  # for position and margin updates
    for symbol in symbols:
        xs.subscribe_to_own_market(symbol)

    if p.contains("no-xs", "no_xs"):
        sl_config["start_xs"] = False
    if p.contains("confirm"):
        sl_config["confirm"] = True
    sl = StopLosser(xs, sl_config)
    sl.start()

    try:
        asyncio.get_event_loop().run_until_complete(sl.closed.wait())
    finally:
        sl.gui.root.quit()


if __name__ == "__main__":
    main()
