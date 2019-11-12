import os
from uxs.base._settings import (SETTINGS, read_settings, set,
                                get_cache_dir, set_cache_dir,
                                get_profiles_dir, set_profiles_dir,
                                get_enable_caching, set_enable_caching,
                                get_enable_caching_for_exchanges, set_enable_caching_for_exchanges,
                                get_cache_expiry, set_cache_expiry,
                                get_cache_expiry_for_exchanges, set_cache_expiry_for_exchanges,
                                clear_setting, clear_settings, APPDATA_DIR, _SETTINGS_INITIAL)
from uxs.base.poll import is_caching_enabled, get_cache_expiry as _get_cache_expiry
from .conftest import _init

test_dir, settings_test_path = _init()


def test_set_profiles_dir(init):
    set_profiles_dir(test_dir, True)
    
    assert SETTINGS['PROFILES_DIR'] == test_dir
    assert read_settings()['PROFILES_DIR'] == test_dir
    assert get_profiles_dir() == test_dir
    
    
def test_set_enable_caching(init):
    set_enable_caching({'orderbook': True, 'ticker': False}, True)
    set_enable_caching_for_exchanges({'bitfinex': {'orderbook': False},
                                      'binance': {'orderbook': True, 'ticker': True}},
                                      True)
    
    assert is_caching_enabled('random_exchange', 'orderbook')
    assert not is_caching_enabled('random_exchange', 'ticker')
    assert not is_caching_enabled('bitfinex', 'orderbook')
    assert not is_caching_enabled('bitfinex', 'ticker')
    assert is_caching_enabled('binance','orderbook')
    assert is_caching_enabled('binance', 'ticker')
    
    clear_setting(['enable_caching_for_exchanges','bitfinex'], True)
    
    assert is_caching_enabled('bitfinex','orderbook')
    
    clear_setting(['enable_caching_for_exchanges','binance','ticker'], True)
    
    assert is_caching_enabled('binance','orderbook')
    assert not is_caching_enabled('binance', 'ticker')
    
    clear_setting('enable_caching', True)
    
    assert not is_caching_enabled('random_exchange', 'orderbook')
    assert not is_caching_enabled('random_exchange', 'ticker')
    
    
def test_set_cache_expiry(init):
    set_cache_expiry({'orderbook': None, 'ticker': 10, 'markets': 20, 'tickers': -1}, True)
    set_cache_expiry_for_exchanges({'bitfinex': {'orderbook': 5},
                                    'binance': {'orderbook': -1, 'tickers': None}},
                                     True)
    
    assert _get_cache_expiry('random_exchange', 'orderbook') == 0
    assert _get_cache_expiry('bitfinex', 'orderbook') == 5
    assert _get_cache_expiry('bitfinex', 'markets') == 20
    assert _get_cache_expiry('bitfinex', 'tickers') == -1
    assert _get_cache_expiry('binance', 'orderbook') == -1
    assert _get_cache_expiry('binance', 'tickers') == -1
    
    clear_setting(['cache_expiry_for_exchanges','bitfinex'], True)
    
    assert _get_cache_expiry('bitfinex', 'orderbook') == 0
    
    clear_setting(['cache_expiry_for_exchanges','binance','orderbook'], True)
    
    assert _get_cache_expiry('binance', 'orderbook') == 0
    
    clear_setting('cache_expiry', True)
    
    assert _get_cache_expiry('random_exchange', 'markets') == 1800


def test_clear_settings(init):
    clear_settings(True)
    
    assert get_cache_dir() == APPDATA_DIR
    assert read_settings()['CACHE_DIR'] == APPDATA_DIR
    assert read_settings()['CACHE_EXPIRY_FOR_EXCHANGES'] == {}