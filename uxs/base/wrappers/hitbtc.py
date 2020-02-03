class hitbtc:
    def describe(self):
        config = {
            'commonCurrencies': {
                'BCC': 'Bitconnect',
            },
        }
        return self.deep_extend(super().describe(), config)
