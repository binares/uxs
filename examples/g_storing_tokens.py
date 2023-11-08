import uxs
import os
import yaml

TOKENS_PATH = os.path.join(os.getcwd(), "uxs_test_tokens.yaml")
ENCRYPTED_PATH = None

# PERMISSIONS: info < trade < withdraw
TOKENS = {
    #            contains permissions   the required apiKey and secret       extra tokens
    "kucoin": [
        {"id": "kucoin_trade_0", "apiKey": "abcd", "secret": "1234", "password": "xyz"}
    ],
    "bitmex": [{"id": "bitmex_withdraw_0_test", "apiKey": "OPQR", "secret": "9876"}],
}
ENCRYPTION_PASSWORD = "myPassword"


def verify(xs):
    entry = TOKENS[xs.exchange][0]
    assert xs.api.apiKey == entry["apiKey"]
    assert xs.api.secret == entry["secret"]
    if "password" in entry:
        assert xs.api.password == entry["password"]
    assert xs.auth_info["id"] == entry["id"]


def main():
    global ENCRYPTED_PATH
    with open(TOKENS_PATH, "w") as f:
        yaml.dump(TOKENS, f)

    # Choosing to make the path permanent will no more require
    # to set it at the start of each session
    uxs.set_tokens_path(TOKENS_PATH, make_permanent=False)

    xs = uxs.get_streamer("kucoin:TRADE")
    verify(xs)

    # Generally there is no point of encrypting the tokens file, unless
    # someone else has access to your computer.
    # In that case this is how it can be done:
    ENCRYPTED_PATH = uxs.encrypt_tokens(ENCRYPTION_PASSWORD)
    # Encrypted file must end with "_encrypted" (don't change it), otherwise
    # uxs will think its an unencrypted file while trying to read tokens from it.
    assert ENCRYPTED_PATH == os.path.join(os.getcwd(), "uxs_test_tokens.yaml_encrypted")
    uxs.set_tokens_path(ENCRYPTED_PATH)
    uxs.cache_password(None)

    # If you want you can now delete the old file.
    # Make sure to remember the password though, or your tokens will be lost.
    os.remove(TOKENS_PATH)

    # Will throw ValueError as the password hasn't been cached
    try:
        uxs.get_auth("kucoin:trade")
    except ValueError as e:
        print(repr(e))

    # Now you'll have to cache the password at the start of each session
    uxs.cache_password(ENCRYPTION_PASSWORD)

    uxs.get_auth("kucoin:trade")

    # The lookup id may contain permissions, adding 'kucoin_0' to it is optional
    # (should you you want to look up that exact tokens entry)
    # Since info < trade, our TOKENS entry qualifies
    xs = uxs.kucoin({"auth": {"id": "info"}})
    verify(xs)

    # For retrieving test tokens, the id must contain "test" part
    uxs.get_auth("bitmex:trade_test")

    # Test tokens are strictly separated from ordinary ones
    try:
        uxs.get_auth("bitmex:trade")
    except ValueError as e:
        print(repr(e))

    # If we initiate a testnet socket ({'test': True}), then it will automatically look up test tokens
    xs2 = uxs.bitmex({"test": True, "auth": {"withdraw": True}})
    verify(xs2)

    # Should you want to decrypt the file
    new_path = uxs.decrypt_tokens()
    assert new_path == TOKENS_PATH


def cleanup():
    try:
        os.remove(TOKENS_PATH)
    except OSError:
        pass

    if ENCRYPTED_PATH is not None:
        try:
            os.remove(ENCRYPTED_PATH)
        except OSError:
            pass


if __name__ == "__main__":
    try:
        main()
    finally:
        cleanup()
