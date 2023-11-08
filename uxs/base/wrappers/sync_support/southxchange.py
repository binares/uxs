class southxchange:
    def create_order(self, symbol, type, side, amount, price=None, params={}):
        o = super().create_order(symbol, type, side, amount, price, params)
        o["id"] = o["id"].strip('"')
        return o
