import prj,os
P = prj.qs(__file__)

import inspect
import datetime
dt = datetime.datetime

TOKEN_PATH = os.path.join(P.mdpath,'tokens\\crypto')
EXTRA_PARAMS = {'kucoin': ['password']}
_warned = {}
_len = len

from fons.io import read_params
import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)


def _verify_credentials(credentials, exchange, i):
    must_have = ('apiKey','secret','withdraw','trade','info','active')
    missing = tuple(x for x in must_have if x not in credentials)
    
    if exchange not in _warned:
        _warned[exchange] = []
        
    if len(missing):
        _we = _warned.get(exchange, _warned.update({exchange:[]} or _warned[exchange]))
        label = '\'{}\''.format(credentials['id']) if credentials.get('id') is not None else 'nr {}'.format(i)
        if i not in _we:
            logger.warning('Token {} of exchange \'{}\' missing the following params: {}'.format(label,exchange,missing))
            _we.append(i)
            
        return False
    
    return True


def _get_prev_frame():
    f = inspect.currentframe()
    filename = os.path.normcase(f.f_code.co_filename)
    while os.path.normcase(f.f_code.co_filename) == filename:
        f2 = f.f_back
        if f2 is None: break
        else: f = f2
    return f


def read_tokens():
    return {e: [c for i,c in enumerate(creds_all) if _verify_credentials(c,e,i)]
        for e,creds_all in read_params(TOKEN_PATH).items()}


def get_auth(exchange, info=True, trade=True, withdraw=False, id=None, user=None, active=True):
    required = ['apiKey','secret'] + EXTRA_PARAMS.get(exchange.lower(),[])
    auth2 = get_auth2(exchange, info, trade, withdraw, id, user, active)
    return {x:y for x,y in auth2.items() if x in required}
    
    
def get_auth2(exchange, info=True, trade=True, withdraw=False, id=None, user=None, active=True):
    e_lower = exchange.lower()
    
    if not any((info,trade,withdraw)):
        return {'apiKey': '', 'secret': '', 'info': False, 'trade': False, 'withdraw': False, 
                'id': '{}_public'.format(exchange), 'user': None, 'active': True}
    
    tokens = read_tokens()

    if e_lower not in tokens:
        raise ValueError('Exchange \'{}\' has no tokens associated with it.'.format(exchange))
    
    e_creds = tokens[e_lower]
    
    _specs = {'info': info, 'trade': trade, 'withdraw': withdraw, 'user': user, 'id': id, 'active': active}
    specs = {x:y for x,y in _specs.items() if y is not None}
    ranks = ('withdraw','trade','info')
    
    #All matches (including those that have more rights than requested)
    matches = [x for x in e_creds if all(x.get(y)==z for y,z in specs.items() if y not in ranks or z is not False)]
    
    #Of those, select the one with least rights
    for rank in ranks:
        disabled = [x for x in matches if x.get(rank) is False]
        if len(disabled): matches = disabled
        else: continue
    
    required = ['apiKey','secret'] + EXTRA_PARAMS.get(exchange,[])
    
    if not len(matches):
        raise ValueError('No {} token matched against these params: {}'.format(exchange, specs))
    elif any(x not in matches[0] for x in required):
        raise TypeError('Token is missing one of these properties: {}'.format(required))
    
    f = _get_prev_frame()
    logger.debug('Retrieved auth id \'{}\' (called from \'\\{}:{}\')'.format(
        matches[0].get('id'), '\\'.join(f.f_code.co_filename.split('\\')[-3:]), f.f_code.co_name))
    
    return matches[0]
