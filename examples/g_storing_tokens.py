import uxs
import os
import yaml

TOKENS_PATH = os.path.join(os.getcwd(), 'uxs_test_tokens.yaml')
ENCRYPTED_PATH = None

#PERMISSIONS: info < trade < withdraw
TOKENS = {
    #            contains permissions   the required apiKey and secret       extra tokens
    'kucoin': [{'id': 'kucoin_trade_0', 'apiKey': 'abcd', 'secret': '1234', 'password': 'xyz'}],
}
ENCRYPTION_PASSWORD = 'myPassword'


def verify(xs):
    assert xs.api.apiKey == 'abcd'
    assert xs.api.secret == '1234'
    assert xs.api.password == 'xyz'


def main():
    global ENCRYPTED_PATH
    with open(TOKENS_PATH, 'w') as f:
        yaml.dump(TOKENS, f)
    
    # Choosing to make the path permanent will no more require
    # to set it at the start of each session
    uxs.set_tokens_path(TOKENS_PATH, make_permanent=False)
    
    xs = uxs.get_socket('kucoin:TRADE')
    verify(xs)
    
    # Generally there is no point of encrypting the tokens file, unless
    # someone else has access to your computer. 
    # In that case this is how it can be done:
    ENCRYPTED_PATH = uxs.encrypt_tokens(ENCRYPTION_PASSWORD)
    # Encrypted file must end with "_encrypted" (don't change it), otherwise
    # uxs will think its an unencrypted file while trying to read tokens from it.
    assert ENCRYPTED_PATH == os.path.join(os.getcwd(), 'uxs_test_tokens.yaml_encrypted')
    uxs.set_tokens_path(ENCRYPTED_PATH)
    
    # If you want you can now delete the old file.
    # Make sure to remember the password though, or your tokens will be lost.
    os.remove(TOKENS_PATH)
    
    # Will throw ValueError as the password hasn't been cached
    try:
        uxs.get_auth('kucoin:trade')
    except ValueError as e:
        print(e)
    
    # Now you'll have to cache the password at the start of each session
    uxs.cache_password(ENCRYPTION_PASSWORD)
    
    uxs.get_auth('kucoin:trade')
    
    # The lookup id may contain permissions, adding 'kucoin_0' to it is optional
    # (should you you want to look up that exact tokens entry)
    # Since info < trade, our TOKENS entry qualifies
    xs = uxs.kucoin({'auth': {'id': 'info'}})
    verify(xs)
    
    # Should you want to decrypt the file
    new_path = uxs.decrypt_tokens()
    assert new_path == TOKENS_PATH


def cleanup():
    try: os.remove(TOKENS_PATH)
    except OSError: pass
    
    if ENCRYPTED_PATH is not None:
        try: os.remove(ENCRYPTED_PATH)
        except OSError: pass


if __name__== '__main__':
    try:
        main()
    finally:
        cleanup()
    