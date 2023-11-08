class southxchange:
    def describe(self):
        return self.deep_extend(
            super().describe(),
            {
                "urls": {
                    "v2": "https://www.southxchange.com/api/v2",
                },
            },
        )

    def nonce(self):
        return self.milliseconds()

    def sign(
        self, path, api="public", method="GET", params={}, headers=None, body=None
    ):
        signed = super().sign(path, api, method, params, headers, body)
        if path == "markets" and not self.urls["v2"] in signed["url"]:
            signed["url"] = signed["url"].replace(self.urls["api"], self.urls["v2"])
        return signed
