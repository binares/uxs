import os
import yaml
from copy import deepcopy

from fons.dict_ops import (deep_get, deep_update, DEL)
from fons.iter import flatten
from fons.io import wait_filelock, SafeFileLock
from fons.os import get_appdata_dir

#IN SECONDS
# -1 (or < 0) for never to expire
_DEFAULT_CACHE_EXPIRY = {
    'markets': 1800,
    'currencies': 1800,
    'balances': 0,
    'balances-account': 0,
    'tickers': 0,
    'ticker': 0,
    'orderbook': 0,
    'ohlcv': 0,
    'trades': 0,
}

APPDATA_DIR = get_appdata_dir('uxs-python', make=True)
SETTINGS_PATH = os.path.join(APPDATA_DIR, 'settings.yaml')
SETTINGS = {
    'TOKENS_PATH': None,
    'PROFILES_DIR': None,
    'CACHE_DIR': APPDATA_DIR,
    'ENABLE_CACHING': {},
    'ENABLE_CACHING_FOR_EXCHANGES': {},
    'CACHE_EXPIRY': deepcopy(_DEFAULT_CACHE_EXPIRY),
    'CACHE_EXPIRY_FOR_EXCHANGES': {},
}
_SETTINGS_INITIAL = deepcopy(SETTINGS)

__all__ = ['read_settings', 'load_settings_from_file',
           'get_setting', 'set', 'clear_setting', 'clear_settings'] + \
          ['get_{}'.format(_.lower()) for _ in SETTINGS] + \
          ['set_{}'.format(_.lower()) for _ in SETTINGS]


def read_settings(update_globals=True):
    try:
        wait_filelock(SETTINGS_PATH)
        with open(SETTINGS_PATH, encoding='utf-8') as f:
            settings = yaml.safe_load(f)
            if settings is None:
                settings = {}
            settings = {k: (v if v is not None else deepcopy(_SETTINGS_INITIAL[k]))
                        for k,v in settings.items()}
    except FileNotFoundError:
        with SafeFileLock(SETTINGS_PATH):
            with open(SETTINGS_PATH, 'w', encoding='utf-8') as f:
                #yaml.dump(_SETTINGS_INITIAL, f)
                #settings = deepcopy(_SETTINGS_INITIAL)
                settings = {}
    
    if update_globals:
        SETTINGS.update(settings)
            
    return settings


def load_settings_from_file(pth, make_permanent=False):
    wait_filelock(SETTINGS_PATH)
    with open(pth, encoding='utf-8') as f:
        data = yaml.safe_load(f)
    if data is None:
        data = {}
    
    return set(data, make_permanent)
     

def get_setting(param):
    return SETTINGS[param.upper()]


def _verify_keys(iterable, accepted):
    unknown = [x for x in iterable if x not in accepted and x is not DEL]
    
    if unknown:
        raise ValueError('Got unknown keys: {}'.format(unknown))


def _prepare_mapping(mapping, accepted_keys):
    if DEL(mapping):
        return mapping
    
    if mapping is None:
        mapping = {}
    elif isinstance(mapping, int): #(ints can be interpreted as bool)
        mapping = dict.fromkeys(accepted_keys, mapping)
    elif not isinstance(mapping, dict):
        raise TypeError(type(mapping))
    
    _verify_keys(mapping, accepted_keys)
    
    return mapping


def _prepare_enable_caching(mapping):
    accepted_keys = tuple(_DEFAULT_CACHE_EXPIRY)
    
    return _prepare_mapping(mapping, accepted_keys)


def _prepare_for_exchanges(mapping, f):
    if DEL(mapping):
        return mapping
    elif not isinstance(mapping, dict):
        raise TypeError(mapping)
    
    not_strs = [type(x) for x in mapping if not isinstance(x,str)]
    if not_strs:
        raise TypeError('Exchanges must be strings; got: {}'.format(not_strs))
    
    return {xc: f(xc_mapping) for xc, xc_mapping in mapping.items()}


def _prepare_enable_caching_for_exchanges(mapping):
    return _prepare_for_exchanges(mapping, _prepare_enable_caching)

    
def _prepare_cache_expiry(mapping):
    accepted_keys = tuple(_DEFAULT_CACHE_EXPIRY)
    
    return _prepare_mapping(mapping, accepted_keys)


def _prepare_cache_expiry_for_exchanges(mapping):
    return _prepare_for_exchanges(mapping, _prepare_cache_expiry)
    
    
APPLY = {'ENABLE_CACHING': _prepare_enable_caching,
         'ENABLE_CACHING_FOR_EXCHANGES': _prepare_enable_caching_for_exchanges,
         'CACHE_EXPIRY': _prepare_cache_expiry,
         'CACHE_EXPIRY_FOR_EXCHANGES': _prepare_cache_expiry_for_exchanges,}


def _write_to_file(write):
    prev_settings = read_settings(False)
    write = dict(prev_settings, **write)
    
    with SafeFileLock(SETTINGS_PATH):
        with open(SETTINGS_PATH, 'w', encoding='utf-8') as f:
            yaml.dump(write, f)


def set(settings, make_permanent=False, **kw):
    """
    :type settings: dict
    :param make_permanent: Whether or not the setting is written to settings.yaml,
                           and loaded again next time the app starts.
    Set the following values: TOKENS_PATH, PROFILES_DIR, CACHE_DIR
    """
    settings = {k.lower(): v for k,v in settings.items()}
    
    _map = {k: [k.lower()] for k in SETTINGS}
    _map['PROFILES_DIR'] +=  ['profiles_path']
    _map['CACHE_DIR'] += ['cache_path']
    
    _are_paths = ['TOKENS_PATH','PROFILES_DIR','CACHE_DIR']
    #_deep = ['ENABLE_CACHING']
    
    accepted = tuple(flatten(_map.values()))

    unknown = [x for x in settings if x not in accepted]
    
    if unknown:
        raise ValueError('Got unknown keys: {}'.format(unknown))
    
    write = {}
    
    for key, synonyms in _map.items():
        
        for syn in synonyms:
            
            if syn in settings:
                value = settings[syn]
                
                if DEL(value) or value is None:
                    value = deepcopy(_SETTINGS_INITIAL[key])
                else:
                    if key in _are_paths:
                        value = os.path.realpath(value)
                        
                    if key in APPLY:
                        value = APPLY[key](value)
                        
                    value = deep_update(SETTINGS[key], value, **kw)
                    
                write[key] = value
                SETTINGS[key] = value
            
    
    if make_permanent:
        _write_to_file(write)
            
    
    return write


#Initiate getters and setters

for _ in SETTINGS:
    exec(
    """def get_{name_l}():
        return get_setting('{name}')""".format(name_l=_.lower(), name=_)
    )
    
    exec(
    """def set_{name_l}(value, make_permanent=False):
        return set({{'{name}': value}}, make_permanent)['{name}']""" \
            .format(name_l=_.lower(), name=_)
    )
    


def clear_setting(param, make_permanent=False):
    deep_param = [param] if isinstance(param, str) else param
    
    D = _d = {}
    ln = len(deep_param)
    
    for i,x in enumerate(deep_param):
        _d[x] = {} if i < ln-1 else \
                deepcopy(deep_get([_SETTINGS_INITIAL], deep_param, return2=DEL))
        _d = _d[x]
    
    set(D, make_permanent)#, shallow=deep_param)
    

def clear_settings(make_permanent=False):
    SETTINGS.update(deepcopy(_SETTINGS_INITIAL))
    
    if make_permanent:
        _write_to_file(SETTINGS)
    

#Initiate the previous settings, or create settings file if doesn't exist
read_settings()