import os
import yaml
from fons.os import make_dirpath
import pytest

from .conftest import _init, cleanup
from uxs import get_tokens_path, set_tokens_path, cache_password
import uxs.base.auth as auth
from uxs.base.auth import read_tokens, get_auth2, encrypt_tokens, decrypt_tokens, _read_ordinary, _undo_file_path_creation

test_dir, settings_test_path = _init()
tokens_path = os.path.join(test_dir, 'tokens.yaml')
tokens_encrypted_path = os.path.join(test_dir, 'tokens.yaml_encrypted')
tokens_path2 = os.path.join(test_dir, 'tokens.yaml(2)')
password = 'HB05tU3WL9'
tokens_original = None
tokens_original_txt = None

TOKENS = {
    'binance': [{'id': 'binance_withdraw', 'trade': False,  'apiKey': 'abc', 'secret': 'def'},
                {'id': 'binance_withdraw_2', 'apiKey': 'cba', 'secret': 'fed'}],
    'kucoin': [{'id': 'kucoin_trade-_withdraw-_test', 'apiKey': 'xxx', 'secret': 'yyy', 'password': 'zzz'},
               {'id': 'kucoin_TRADE-_WITHDRAW-', 'apiKey': 'opq', 'secret': 'rst', 'password': '123'}],
    'okex': [{'id': 'okex_disabled', 'apiKey': 'ox_key', 'secret': 'ox_secret'}],
}


@pytest.fixture(scope='module')
def _init_auth(init):
    global tokens_original, tokens_original_txt
    set_tokens_path(tokens_path)
    with open(tokens_path,'w',encoding='utf-8') as f:
        yaml.dump(TOKENS, f)
    tokens_original = read_tokens()
    with open(tokens_path,encoding='utf-8') as f:
        tokens_original_txt = f.read()


def _entry(kw, info=False, trade=False, withdraw=False, active=True, test=False):
    entry = dict(kw, **{'info': info, 'trade': trade, 'withdraw': withdraw, 'active': active, 'test': test})
    entry.update(dict.fromkeys([k for k in ['id','user'] if k not in entry]))
    return entry

#---------------------------------------------------------

EXPECTED_TOKENS = {
    'binance': [_entry(TOKENS['binance'][0],True,False,True),
                _entry(TOKENS['binance'][1],True,True,True)],
    'kucoin': [_entry(TOKENS['kucoin'][0],False,True,True,True,True),
               _entry(TOKENS['kucoin'][1],False,True,True)],   
    'okex': [_entry(TOKENS['okex'][0],False,False,False,False)], 
}

@pytest.mark.parametrize('exchange', ['binance','kucoin'])
def test_read_tokens(exchange, _init_auth):
    retrieved = read_tokens()[exchange]
    
    assert retrieved == EXPECTED_TOKENS[exchange]

#---------------------------------------------------------
    
TEST_AUTH_DATA = [
    [   {'exchange': 'binance', 'id': 'binance_2_withdraw'}, 
        _entry(TOKENS['binance'][1],True,True,True),
    ],
    [   {'exchange': 'binance', 'info': True},
        _entry(TOKENS['binance'][0],True,False,True),
    ],
    [   {'exchange': 'binance', 'apiKey': 'abc', 'secret': 'wrong_secret'},
        _entry(dict(TOKENS['binance'][0], secret='wrong_secret', id=None),False,False,False),
    ],
    [   {'exchange': 'kucoin', 'id': 'withdraw-'},
        _entry(TOKENS['kucoin'][1],False,True,True),
    ],
    [   {'exchange': 'kucoin', 'id': 'withdraw-_test'},
        _entry(TOKENS['kucoin'][0],False,True,True,True,True),
    ],
    [   {'exchange': 'kucoin', 'id': 'withdraw-', 'test': True},
        _entry(TOKENS['kucoin'][0],False,True,True,True,True),
    ],
]


@pytest.mark.parametrize('params, expected', TEST_AUTH_DATA)
def test_auth2(params, expected, _init_auth):
    retrieved = get_auth2(**params)
    
    assert retrieved == expected

#---------------------------------------------------------

def test_cache_password():
    original = auth._password
    cache_password(None)
    assert auth._password is None
    cache_password(password)
    assert auth._password == password
    cache_password(original)
    

ufpc_data = [
    ['x.yaml_encrypted','_encrypted','x.yaml'],
    ['x.yaml_ENCRYPTED','_eNcRyPteD','x.yaml'],
    ['x.yaml_encrypted(2)','_encrypted','x.yaml(2)'],
    ['x.yaml_encrypted((2)','_encrypted','x.yaml_encrypted((2)'],
    ['x.yaml_','','x.yaml_'],
]

@pytest.mark.parametrize('fname, ending, expected_fname', ufpc_data)
def test__undo_file_path_creation(fname, ending, expected_fname):
    dirn = 'aDirectory'
    fpath = os.path.join(dirn, fname)
    expected_path = os.path.join(dirn, expected_fname)
    assert _undo_file_path_creation(fpath, ending) == expected_path
    
#---------------------------------------------------------

def _read_txt(pth):
    with open(pth, encoding='utf-8') as f:
        return f.read()


def test_encrypt_tokens():
    _encryped_path = encrypt_tokens(password)
    assert _encryped_path == tokens_encrypted_path
    assert auth._password == password
    
    set_tokens_path(tokens_encrypted_path, True)
    
    #Verify that the file contains only code
    with open(tokens_encrypted_path, 'rb') as f:
        txt = yaml.safe_load(f)
        assert isinstance(txt, str)
        assert ' ' not in txt
        assert 'binance' not in txt

    tokens = read_tokens()
    assert tokens == tokens_original
    
    #verify that the old file hasn't been modified
    assert _read_txt(tokens_path) == tokens_original_txt
    

def test_decrypt_tokens():
    cache_password(password)
    _decrypted_path = decrypt_tokens()
    assert _decrypted_path == tokens_path2
    set_tokens_path(tokens_path2, True)
    
    tokens = read_tokens()
    
    assert tokens == tokens_original
    

    
    