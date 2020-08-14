import os
import inspect
import yaml
from io import BytesIO
from collections import defaultdict
import datetime
dt = datetime.datetime

from ._settings import get_setting, set as _set

from fons.crypto import password_encrypt, password_decrypt
from fons.io import wait_filelock, SafeFileLock
import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)

EXTRA_TOKEN_KEYWORDS = {
    'bitclave': ['uid'],
    'kucoin': ['password'],
}
DELETE_DAMAGED_DESTINATION_FILE = True
ALL_DESCRIPTORS = ['test','null','info','trade','withdraw','info-','trade-','withdraw-']

_password = None
_warned = defaultdict(list)
_len = len

def _verify_exchange_entry(entry, exchange, i):
    _update_entry_by_its_id(entry)
    must_have = ('apiKey','secret')
    missing = tuple(x for x in must_have if x not in entry)
    
    if exchange not in _warned:
        _warned[exchange] = []
        
    if len(missing):
        _we = _warned[exchange]
        label = "'{}'".format(entry['id']) if entry.get('id') else "#{}".format(i)
        if i not in _we:
            logger.warning("Token {} of exchange '{}' missing the following params: {}".format(label, exchange, missing))
            _we.append(i)
            
        return False
    
    return True


def _update_entry_by_its_id(credentials):
    interpretation = _interpret_auth_id(credentials.get('id'))
    #Do not overwrite already defined keywords
    not_already_defined = {x:y for x,y in interpretation.items() if credentials.get(x) is None}
    credentials.update(not_already_defined)
    
    if 'user' not in credentials:
        credentials['user'] = None
    
    return credentials


def _get_prev_frame():
    f = inspect.currentframe()
    filename = os.path.normcase(f.f_code.co_filename)
    while os.path.normcase(f.f_code.co_filename) == filename:
        f2 = f.f_back
        if f2 is None: break
        else: f = f2
    return f


def _interpret_exchange(exchange, id=''):
    if isinstance(exchange, str) and ':' in exchange:
        loc = exchange.find(':')
        if len(exchange[loc+1:]):
            id = exchange[loc+1:]
        exchange = exchange[:loc]
    return exchange, id


def _interpret_auth_id(id):
    if id is None: id = ''
    specs = id.lower().split('_')
    by_rank = ['info','trade','withdraw']
    
    d = {}
    
    d['active'] = not any(x in specs for x in ('disabled','inactive'))
    d['test'] = 'test' in specs
    
    rights = dict.fromkeys(by_rank, False)
    
    for rank in filter(lambda x: x in specs, ALL_DESCRIPTORS[2:]):
        include_lower_ranks = True
        
        if rank.endswith('-'):
            rank = rank[:-1]
            include_lower_ranks = False
        
        if include_lower_ranks:
            index = by_rank.index(rank)
            changed = dict.fromkeys(by_rank[:index+1], True)
        else:
            changed = {rank: True}
        
        rights.update(changed)
    
    return dict(d, **rights)


def _strip_id_from_descriptors(id):
    if id is None: id= ''
    return '_'.join([x for x in id.split('_') if x.lower() not in ALL_DESCRIPTORS])


def _id_labels_match(id1, id2):
    _id1 = _strip_id_from_descriptors(id1)
    _id2 = _strip_id_from_descriptors(id2)
    return _id1 == _id2


def read_tokens(password=None):
    tokens_path = get_setting('TOKENS_PATH')
    
    if tokens_path is not None:
        is_encrypted = tokens_path.endswith('_encrypted')
        if not is_encrypted:
            data = _read_ordinary(tokens_path)
        else:
            data = _read_encrypted(tokens_path, password)
        tokens = {xc: [e for i,e in enumerate(entries) if _verify_exchange_entry(e,xc,i)]
                  for xc,entries in data.items()}
    else:
        tokens = {}
        
    return tokens


def _read_ordinary(tokens_path):
    wait_filelock(tokens_path)
    with open(tokens_path, encoding='utf-8') as f:
        tokens = yaml.safe_load(f)
    if tokens is None:
        tokens = {}
        
    return tokens


def get_auth(exchange, id=None, *,
             info=None, trade=None, withdraw=None,
             test=None, user=None, active=None, **kw):
    """Get keys (apiKey, secret, password, etc..) only"""
    exchange, id = _interpret_exchange(exchange, id)
    required = ['apiKey','secret'] + EXTRA_TOKEN_KEYWORDS.get(exchange.lower(),[])
    auth2 = get_auth2(exchange, id, info=info, trade=trade, withdraw=withdraw,
                      test=test, user=user, active=active, **kw)
    
    return {x:y for x,y in auth2.items() if x in required}
    
    
def get_auth2(exchange, id=None, *,
              info=None, trade=None, withdraw=None,
              test=None, user=None, active=None, **kw):
    """Get full auth"""
    exchange, id = _interpret_exchange(exchange, id)
    requested = dict({'id': id, 'info': info, 'trade': trade, 'withdraw': withdraw,
                      'test': test, 'active': active, 'user': user}, **kw)
    
    _update_entry_by_its_id(requested)
    
    if _are_tokens_already_set(requested):
        return _add_auth(exchange, requested)
    
    info, trade, withdraw = requested['info'], requested['trade'], requested['withdraw']
    rights = {k: requested[k] for k in ['info','trade','withdraw']}
    
    if not any(rights.values()):
        if not id:
            id = '{}_NULL'.format(exchange)
        #elif 'NULL' not in id.upper().split('_'):
        #    id += '_NULL'
            
        return {'id': id, 'apiKey': '', 'secret': '',
                'info': False, 'trade': False, 'withdraw': False, 
                'test': None, 'user': None, 'active': True}
    
    xc_lower = exchange.lower()
    tokens = read_tokens()

    if xc_lower not in tokens:
        raise ValueError("Exchange '{}' has no tokens associated with it.".format(exchange))
    
    entries = tokens[xc_lower]
    entries = [_update_entry_by_its_id(e) for e in entries]
    
    _specs = {k:requested[k] for k in ['test','info','trade','withdraw','user','active']}
    specs = {x:y for x,y in _specs.items() if y is not None}
    show_specs = dict(specs, id=id)
    ranks_from_highest = ('withdraw','trade','info')
    matches = entries
    
    id_plain = _strip_id_from_descriptors(id)
    if id_plain:
        matches = [e for e in matches if _id_labels_match(e.get('id'), id_plain)]
    
    # All matches (including those that have more rights than requested)
    matches = [e for e in matches if all(e.get(k)==v for k,v in specs.items()
                                         if k not in ranks_from_highest or v is not False)]
    
    # Of those, select the one with least rights
    for rank in ranks_from_highest:
        negative = [e for e in matches if not e.get(rank)]
        if len(negative):
            matches = negative
    
    required = ['apiKey','secret'] + EXTRA_TOKEN_KEYWORDS.get(exchange,[])
    
    if not len(matches):
        raise ValueError("No {} token matched against these params: {}".format(exchange, show_specs))
    elif any(x not in matches[0] for x in required):
        match_id = matches[0].get('id')
        raise TypeError("Token {}:{} is missing one of these properties: {}".format(xc_lower, match_id, required))
    
    f = _get_prev_frame()
    entry = matches[0]
    
    # Log the id of the matched entry and what function and module requested it
    tlogger0.debug("Retrieved auth id '{}' (called from '/{}:{}')".format(
        entry.get('id'), '/'.join(f.f_code.co_filename.split(os.sep)[-3:]), f.f_code.co_name))
    
    return entry


def _are_tokens_already_set(kw):
    if kw.get('apiKey'):
        return True
    else:
        return False


def _add_auth(exchange, kw):
    required = ['apiKey','secret'] + EXTRA_TOKEN_KEYWORDS.get(exchange.lower(),[])
    kw.update({k:'' for k in required if not kw.get(k)})
    return kw


def _create_file_path(path, ending=''):
    dir = os.path.dirname(path)
    fn = os.path.basename(path)
    path += ending
    i = 2
    
    while os.path.exists(path):
        new_fn = '{}{}({})'.format(fn,ending,i)
        i += 1
        path = os.path.join(dir, new_fn)
        
    return path


def _undo_file_path_creation(path, ending=''):
    """tokens_encrypted(2) -> tokens(2) | ending='encrypted'"""
    if not ending:
        return path
    
    dir = os.path.dirname(path)
    fn = os.path.basename(path)
    
    if ending.lower() not in fn.lower():
        return path
    
    loc = fn.lower().rfind(ending.lower())
    end = loc + len(ending)
    
    after = fn[end:]

    if after and (after[:1] != '(' or after[-1:] != ')' or not after[1:-1].isdigit()):
        return path
    
    return os.path.join(dir, fn[:loc]+after)
    

def _fetch_password(password):
    if password is None:
        if _password is None:
            raise ValueError("Password isn't specified, nor has been cached globally")
        password = _password
        
    if not isinstance(password, str):
        raise TypeError('Password must be string; got: {}'.format(type(password)))
        
    return password


def _fetch_tokens_path():
    tokens_path = get_setting('TOKENS_PATH')
    if tokens_path is None:
        raise ValueError("TOKENS_PATH isn't set")
    
    return tokens_path
    

def encrypt_tokens(password, destination_dir=None, **kw):
    """
    Encrypts the file against the password. Output file will be named as 
    "{original_name}_encrypted", and saved in the same directory, unless
    destination_dir is specified. Original file will NOT be modified.
    
    :returns: the path to the new file (encrypted)
    """
    if not isinstance(password, str):
        raise TypeError('Password must be string; got: {}'.format(type(password)))
    
    #To ensure that the file is not already encrypted, nor badly formatted
    read_tokens()
    
    tokens_path = os.path.realpath(_fetch_tokens_path())
    _pth = os.path.join(destination_dir, os.path.basename(tokens_path)) \
            if destination_dir is not None else tokens_path
    write_path = _create_file_path(_pth, '_encrypted')
        
    unknown = [x for x in kw if x not in ('iterations',)]
    
    if unknown:
        raise ValueError('Got unknown kwarg(s): {}'.format(unknown))
    
    tokens = _read_ordinary(tokens_path)
    
    encrypted = password_encrypt(yaml.dump(tokens), password, **kw)
    
    #Remember the password in-app
    cache_password(password) 
    
    with SafeFileLock(write_path):
        with open(write_path, 'wb') as f:
            f.write(encrypted)
            
    #Make sure that the file wasn't damaged in the process
    if _read_encrypted(write_path) != tokens:
        if DELETE_DAMAGED_DESTINATION_FILE:
            os.remove(write_path)
        raise OSError('The destination file was damaged in the process')
        
    return write_path


def _read_encrypted(tokens_path, password=None):
    password = _fetch_password(password)
    
    wait_filelock(tokens_path)
    
    with open(tokens_path,'rb') as f:
        decrypted = password_decrypt(f.read(), password)
    
    with BytesIO(decrypted) as stream:
        tokens = yaml.safe_load(stream)
    
    if tokens is None:
        tokens = {}
    
    #Remember the password in-app 
    cache_password(password)
    
    return tokens


def decrypt_tokens(password=None, destination_dir=None):
    """
    Decrypts the file. Output file will be named as "{file_name}"
    with its "_enrypted" ending stripped, and saved in the same directory,
    unless destination_dir is specified. Original file will NOT be modified.
    
    :returns: the path to the new file (no encryption)
    """
    tokens_path = os.path.realpath(_fetch_tokens_path())
    password = _fetch_password(password)
    #encrypted = get_setting('ENCRYPTED')
    is_encrypted = tokens_path.endswith('_encrypted')
    
    if not is_encrypted:
        raise ValueError('File "{}" is not encrypted.'.format(tokens_path))
    
    _pth = os.path.join(destination_dir, os.path.basename(tokens_path)) \
            if destination_dir is not None else tokens_path
    _pth = _undo_file_path_creation(_pth, '_encrypted')
    new_path = _create_file_path(_pth, '')
    
    tokens = _read_encrypted(tokens_path, password)
    
    with SafeFileLock(new_path):
        with open(new_path, 'w') as f:
            yaml.dump(tokens, f)
    
    #Make sure that the file wasn't damaged in the process
    if _read_ordinary(new_path) != tokens:
        if DELETE_DAMAGED_DESTINATION_FILE:
            os.remove(new_path)
        raise OSError('The destination file was damaged in the process')
        
    #Remember the password in-app
    cache_password(password)
        
    return new_path
    

def cache_password(password):
    global _password
    _password = password
    
    
    
    