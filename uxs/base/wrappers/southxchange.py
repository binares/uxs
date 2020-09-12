import os
import json

path = os.path.join(os.path.dirname(__file__), '..', '..', '_data', 'southxchange_markets.json')
with open(path, encoding='utf-8') as f:
    markets = json.load(f)


class southxchange:
    def describe(self):
        return self.deep_extend(
            super().describe(),
            {
                'urls': {
                    'v2': 'https://www.southxchange.com/api/v2',
                },
                'options': {
                    'markets': markets,
                },
            } 
        )
    
    def sign(self, path, api='public', method='GET', params={}, headers=None, body=None):
        signed = super().sign(path, api, method, params, headers, body)
        if path == 'markets' and not self.urls['v2'] in signed['url']:
            signed['url'] = signed['url'].replace(self.urls['api'], self.urls['v2'])
        return signed
