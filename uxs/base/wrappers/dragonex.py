import datetime


class dragonex:
    @staticmethod
    def gmt(timestamp):
        utc_datetime = datetime.datetime.utcfromtimestamp(int(round(timestamp / 1000)))
        return utc_datetime.strftime("%a, %d %b %Y %H:%M:%S GMT")
