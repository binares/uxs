class coinbene:
    def describe(self):
        return self.deep_extend(
            super().describe(),
            {
                'options': {
                    'maxDivergence': {'buy': 0.15, 'sell': 0.15, 'minTimesSpread': 2}, # the actual is 0.2 (80%, 120%)
                },
            }
        )
