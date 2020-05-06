class bittrex:
    def describe(self):
        config = {
            'hostname': 'global.bittrex.com',
        }
        return self.deep_extend(super().describe(), config)
