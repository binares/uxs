import uxs


def test_yf_download():
    currencies = ["DOT", "ICP"]

    since = "2023-01-01"
    until = "2024-04-29"
    df = uxs.fintls.ohlcv.yf_download(
        uxs.get_sn_exchange("binance"), currencies, since, until
    )
    print(df)
