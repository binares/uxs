import os, sys
import uxs

exchange_list = uxs.list_streaming_exchanges()
fields = ['ticker', 'all_tickers', 'orderbook', 'l3', 'ohlcv', 'trades', 'balance', 'order', 'fill', 'position']

path = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', 'README.md'))
with open(path, encoding='utf-8') as f:
    txt = f.read()

find = '\nSupported exchanges:\n'
assert find in txt
from_loc = txt.find(find) + len(find)

to_loc = from_loc

if len(sys.argv) == 1 or sys.argv[1] == '0':
    for ln in txt.split('\n'):
        if ln.startswith('|') and ln.endswith('|') and ln.count('|') == len(fields)+2:
            to_loc = txt.find(ln) + len(ln)
else:
    for exchange in exchange_list:
        find = '\n| {} '.format(exchange)
        if find in txt:
            to_loc = txt.find(find)
            nxt_break = txt[to_loc+1:].find('\n')
            to_loc += 1 + nxt_break

longest_exchange_id = max(map(len, exchange_list))

def generate_line(exchange):
    xs = uxs.get_streamer(exchange)
    exchange_cell = ' ' + exchange + ' ' * (longest_exchange_id - len(exchange) + 1)
    values = [exchange_cell]
    for f in fields:
        has = xs.has_got(f)
        if not has:
            value = ''
        elif xs.has[f]['emulated'] == 'poll':
            value = 'p' # emulated via polling ccxt fetch methods
        elif f in ('order', 'fill'):
            if xs.has[f]['emulated'] != 'account':
                value = 'o' # must be subscribed to `own_market`
            else:
                value = '+'
        elif xs.has[f]['emulated']:
            value = 'w' # emulated but via streaming
        else:
            value = '+'
        add_len = len(f) + 2 - len(value)
        add_left = int(add_len / 2)
        add_right = add_len - add_left
        value = (' ' * add_left) + value + (' ' * add_right)
        values.append(value)
    return '|' + '|'.join(values) + '|'

column_line = '| ' + (longest_exchange_id * ' ') + ' | ' + ' | '.join(fields) + ' |'
line_line = '|-' + (longest_exchange_id * '-') + '-|-' + '-|-'.join('-'*len(f) for f in fields) + '-|'
exchange_lines = [generate_line(exchange) for exchange in exchange_list]
specs_txt = '\n' + '\n'.join([column_line, line_line, *exchange_lines])

new_txt = txt[:from_loc] + specs_txt + txt[to_loc:]

with open(path, 'w', encoding='utf-8') as f:
    f.write(new_txt)

