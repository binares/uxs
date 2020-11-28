# UXS - Unified eXchange Streaming
A unified crypto exchange websocket

This free to use python library is ccxt based. I use it for personal trading and data feeds. Pull requests are welcome, although I'll yet have to make some sort of guide on how to implement new exchanges.

**DISCLAIMER: Use it at your own risk! I take no responsibility for any losses occurred!**

Supported exchanges:

|              | ticker | all_tickers | orderbook | l3 | ohlcv | trades | balance | order | fill | position |
|--------------|--------|-------------|-----------|----|-------|--------|---------|-------|------|----------|
| binance      |   +    |      +      |     +     |    |   +   |   +    |    +    |   +   |  p   |          |
| binancefu    |   +    |      +      |     +     |    |   +   |   +    |    +    |   +   |  p   |    +     |
| bitmex       |   p    |      +      |     +     |    |   p   |   p    |    +    |   o   |  o   |    +     |
| bittrex      |   p    |      +      |     +     |    |   p   |   p    |    +    |   +   |  p   |          |
| bw           |   +    |      +      |     +     |    |   +   |   +    |    p    |   p   |      |          |
| coinbene     |   +    |      +      |     +     |    |   +   |   +    |    p    |   p   |      |          |
| coindcx      |   p    |      p      |     +     |    |   p   |   +    |    +    |   p   |  +   |          |
| gateiofu     |        |             |     +     |    |   p   |        |         |       |      |          |
| hitbtc       |   +    |      p      |     +     |    |   p   |   p    |    p    |   +   |  +   |          |
| kraken       |   +    |      p      |     +     |    |   +   |   +    |         |   +   |  +   |          |
| krakenfu     |   +    |      p      |     +     |    |   p   |   +    |    +    |   +   |  +   |    +     |
| kucoin       |   +    |      +      |     +     |    |   p   |   +    |    +    |   o   |  o   |          |
| luno         |   p    |      p      |     w     | +  |   p   |   w    |    p    |   o   |  o   |          |
| poloniex     |   p    |      +      |     +     |    |   p   |   p    |    +    |   +   |  +   |          |
| southxchange |   p    |      p      |     +     |    |   p   |   +    |    p    |   p   |      |          |

Note that there aren't separate subscription channels for *balance*, *order*, *fill* (your trade) and *position* - they all belong under *account*: `xs.subscribe_to_account()`. However some exchanges like bitmex require you to subscribe to each market directly for order and fill updates: `xs.subscribe_to_own_market(symbol)` (but you'll still want to also subscribe to account, as it contains balance and position updates). 

*+* - direct streaming\
*p* - emulated via polling (fetch_balance, fetch_tickers etc)\
*w* - emulated via streaming (l3 -> orderbook, l3 -> trades)\
*o* - must be subscribed to `own_market`

Test environments are currently offered for: BinanceFu, BitMEX, KrakenFu and Kucoin (Kraken also offers some sort of non-sandboxed test env). For using them add `{'test': True}` to the config, and make sure you have created a sandbox account.

Simple usage:

```
import uxs
import asyncio

xs = uxs.binance({'apiKey': '', 'secret': ''})
xs.subscribe_to_orderbook('BTC/USDT')
# Subscribes to balance, order, fill and position updates
xs.subscribe_to_account()
xs.start()

asyncio.get_event_loop().run_forever()
```

## ExchangeSocket

All exchange classes inherit from `uxs.ExchangeSocket`. Note that `uxs.ExchangeSocket` itself *isn't* a subclass of `ccxt.Exchange`, i.e. `uxs.binance` **!**=~ `ccxt.async_support.binance`. Its corresponding (asynchronous) ccxt Exchange instance is accessible under `.api`: `uxs.binance.api` =~ `ccxt.async_support.binance`. The `.api` object's class is a wrapped one through, with some extra attributes added for caching data, rounding up/down, calculating payout and altering the markets data (e.g. for personalized fees).

`ExchangeSocket` does borrow *some* `ccxt.async_support.Exchange` methods: `create_order`, `edit_order`, `cancel_order` will normally evoke the same method of ccxt class, unless the exchange supports socket trading (hitbtc). And `fetch_ticker`, `fetch_tickers`, `fetch_order_book`, `fetch_ohlcv`, `fetch_trades`, `fetch_balance`, `fetch_open_orders`, which may in some cases be evoked through socket (bittrex).

The included examples show how to wait for updates, use callbacks, trade, cache fetch results and store tokens in a file / password encrypted file.

## Subscriptions

```
# Subscribe

xs.subscribe_to_ticker(symbol)

xs.subscribe_to_all_tickers()

xs.subscribe_to_orderbook(symbol)

xs.subscribe_to_l3(symbol)

xs.subscribe_to_ohlcv(symbol, timeframe)

xs.subscribe_to_trades(symbol)

xs.subscribe_to_account()

xs.subscribe_to_own_market(symbol)

# Unsubscribe

xs.unsubscibe_to_{channel}(**params)

# Shortened

xs.sub_to_{channel}(**params)

xs.unsub_to_{channel}(**params)

# Dynamically

xs.subscribe_to({'_': channel, **params})

xs.unsubscribe_to({'_': channel, **params})

# Subscription object

s = xs.get_subscription({'_': channel, **params})
# or
s = xs.get_subscription((channel, *params))

s.state # 0=inactive, 1=active

await s.wait_till_active() # wait till the subscription becomes active
# or
await xs.wait_till_subscription_active({'_': channel, **params})
```

## Data structures

These are attributes of ExchangeSocket instance (xs). The objects in these dicts are equivalent to those of ccxt.Exchange (except for position, which isn't yet implemented by ccxt).

```
xs.tickers[symbol]

xs.orderbooks[symbol]

xs.l3_books[symbol]

xs.ohlcv[symbol][timeframe]

xs.trades[symbol]

xs.balances[currency]

xs.orders[order_id]

xs.open_orders[order_id]

xs.fills[order_id]

xs.positions[symbol]
```

An order also contains 'payout' keyword, which is the current received amount in target currency.

The structures are updated on spot. Bids/asks are inserted directly into the existing list, dict values are updated but the dict objects' id never changes. That includes all sub-level dicts (orders, fills, ...), and even the 'info' dicts (but not the other dicts like 'fee': {'cost': .. , 'currency': ..}). So for any time spanning operation (await create_order()), or if you're accessing the data from another thread, there is a real possibility that the dict has been updated in the meanwhile. To ensure that you'll still have access to the old values, make a (deep)copy of the structure before. Also avoid looping over a structure while for example creating an order (in the loop), as the dict/list size might change, and python throws an error (in dict case).

Note that once a subscription feed is lost/unsubbed, the associated *real-time* data is automatically deleted. This it to prevent the user using outdated data, and by default includes these channels: *all_tickers*, *ticker*, *orderbook*. E.g. once ('orderbook', 'ETH/BTC') is lost, `xs.orderbooks['ETH/BTC']` is deleted. Account relevant data is not deleted, as you might want to cancel the orders / close the positions.

To change it initiate an exchange like this:
```
# all channels
uxs.binance({'channel_defaults': {'delete_data_on_unsub': False}})`
# specific channel
uxs.binance({'channels': {'account': {'delete_data_on_unsub': True}}})
```

## Wait on stream event

```
# See example c / docstring for description
await xs.wait_on(stream, id=-1)
```

## Add stream event callback

```
# See example d for description
xs.add_callback(cb, stream, id=None)
```

## xs.create_order

xs.create_order automatically rounds the price down for buy orders, and up for sell orders. The amount is also rounded, always down.

## Logging

Initialize with `verbose` value 0.5 (create/edit/cancel order), 1 (0.5 + connection, subscription, send, fetch/polling events), 2 (0.5 + 1 + ping + some inner mechanisms) or 3 (0.5 + 1 + 2 + recv events) depending on how detailed log you want. Unexpected errors (not connectivity related) are logged in any case.

```
uxs.binance({'verbose': 1})
```

If you just want to see recv output without including 0.5, 1 and 2, init with

```
uxs.binance({'connection_defaults': {'handle': print}})
```
