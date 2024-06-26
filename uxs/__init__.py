__version__ = "0.7.0"
__author__ = "binares"

import os
import yaml

from uxs.base.socket import ExchangeSocket, ExchangeSocketError

from uxs.base.ccxt import (
    get_name,
    get_exchange,
    get_sn_exchange,
    init_exchange,
    list_exchanges,
    close_all_exchanges,
    ccxtWrapper,
    asyncCCXTWrapper,
    PUBLIC_AUTH_REQUIRED,
)

from uxs.base._settings import *

from uxs.base.auth import (
    read_tokens,
    get_auth,
    get_auth2,
    cache_password,
    encrypt_tokens,
    decrypt_tokens,
    _interpret_exchange,
)

from uxs.base import poll
from uxs.base.poll import fetch, sn_fetch, clear_cache

import uxs.fintls as fintls


from uxs.binance import binance
from uxs.binancefutures import binancefutures
from uxs.bitmex import bitmex
from uxs.hitbtc import hitbtc
from uxs.kraken import kraken
from uxs.krakenfutures import krakenfutures
from uxs.kucoin import kucoin
from uxs.poloniex import poloniex


def list_streaming_exchanges():
    return sorted(
        k
        for k, v in globals().items()
        if isinstance(v, type)
        and issubclass(v, ExchangeSocket)
        and v is not ExchangeSocket
    )


def get_streamer_cls(exchange):
    streamer_cls = globals().get(exchange.lower(), None)

    if not isinstance(streamer_cls, type) or not issubclass(
        streamer_cls, ExchangeSocket
    ):
        raise ValueError("Unknown exchange: {}".format(exchange))

    return streamer_cls


def get_streamer(exchange, config={}):
    """
    Exchange can be given as "exchange_name" or "exchange_name:id_label"
    Config must be dict.

    For authenticating include in config:
        {'apiKey': apiKey, 'secret': secret, ...other exchange specific tokens like "password"}

    or if TOKENS_PATH is set [`uxs.set({'TOKENS_PATH': path_to_the_yaml_file_where_you_keep_your_tokens}`]:

        exchange = "binance:binance_1" -> looks for binance entry with matching id ("binance_1")
                                          (the permission parts of id labels are not compared:
                                           "binance_1" == "binance_1_trade")
        {'auth': {'id': "binance_1"}} -> -||-
        {'auth': "binance_1"}         -> -||-

        "binance:TRADE-" -> looks for (binance) entry that has trade rights, but not necessarily info and withdraw
                            ("-" at the end of a permission means it doesn't include lower permissions)
        {'auth': {'trade': True}} -> -||-

        "binance:TRADE" -> entry must have all permissions <= 'trade', i.e. 'info' and 'trade'

        "binance:binance_INFO-_WITHDRAW-" -> must have 'info' and 'withdraw', and starts with 'binance'

        The key retrieved will always have the least amount of permissions that was requested
            (if 'trade' was requested, but entries 'trade' and 'withdraw' exist,
             then the entry with 'trade' will be retrieved.)

        A test key is only retrieved when explicitly requested:
            "binance:binance_test_1" or {'auth': {'test': True}} or simply {'test': True}
            (the last one also evokes .setup_test_env(), which is probably necessary)

    If the tokens file is encrypted, uxs.cache_password(encryption_password) must be called before.

    :rtype: ExchangeSocket
    """
    exchange, auth_id = _interpret_exchange(exchange)

    if isinstance(config.get("auth"), str):
        config["auth"] = {"id": config["auth"]}

    if auth_id:
        if "auth" not in config:
            config["auth"] = {}
        config["auth"]["id"] = auth_id

    streamer_cls = get_streamer_cls(exchange)

    return streamer_cls(config)


# For backwards compatibility
get_socket_cls = get_streamer_cls
get_socket = get_streamer


def lazy_customize(enable_caching_for=["markets", "currencies", "tickers"]):
    """
    Enable caching - these types will be cached after each fetch;
      and markets/currencies are loaded from cache (discarded and fetched again if age > 30 minutes)
    """
    for item in enable_caching_for:
        set_enable_caching({item: True}, make_permanent=True)


"""
Tokens can be stored in a yaml file with the following format:

exchange_1_name_lowercase: [entry11, entry12, ...]
exchange_2_name_lowercase: [entry21, ...]

where `entry` is a dict with the following keywords:

###EXCHANGE ENTRY KEYWORDS###

#Tokens <str>
#apiKey and secret are compulsory, others vary by exchange 
apiKey: a1bcdef2
secret: ghij34kl
password: mnopq5

#--Optional keywords--
#These are used for lookup executed from config's 'auth' dict [`get_streamer(config={'auth':{..}})`]

#Test <bool>
# whether or not this is a testnet token
test: true

#Rights <bool> 
# these are only used for apiKey and secret lookup by specifying what rights are required
info: true
trade: true
withdraw: false

#Id <str>
# The rights above (and "test") can be left unspecified 
# if the id itself contains the keywords ("info","trade","withdraw","test")
# Keywords must be separated with "_"
# "withdraw" will include lower rights, ("trade","info") as will "trade" ("info")
# in order to NOT include lower rights, add "-" to the end of the keyword ("trade-")
id: binance_trade_1_test

#Active <bool>
#(whether or not the apiKey and secret are still valid)
active: true

#User <str>
#Who is using the APIKEY, this is for self note
user: cryptowat.ch

#############################

The package has a built-in encryption mechanism for encrypting the tokens file (from prying eyes).
To do so call `uxs.encrypt_tokens(a_password)`, and manually delete the original file
(make sure to remember the password, or your tokens will be lost!).
Henceforth `uxs.cache_password(a_password)` must be called each time after importing the package.
To undo the decryption call `uxs.decrypt_tokens()`, and manually delete the encrypted file.
"""
