from .orderbook import OrderbookMaintainer
from uxs.fintls.l3 import create_l3_orderbook, l3_to_l2
import fons.log

logger, logger2, tlogger, tloggers, tlogger0 = fons.log.get_standard_5(__name__)


class L3Maintainer(OrderbookMaintainer):
    config_key = "l3"
    channel = "l3"
    data_key = "l3_books"
    fetch_method = "fetch_l3_order_book"
    update_method = "update_l3_orderbooks"
    name = "l3 ob"

    def build_ob(self, ob):
        return create_l3_orderbook(ob)

    def _push_cache(self, symbol, force_till_id=None):
        is_synced, performed_update = super()._push_cache(symbol, force_till_id)
        if not is_synced and self.is_subbed_to_l2_ob(symbol):
            self.xs.change_subscription_state(("orderbook", symbol), 0)
        return is_synced, performed_update

    @staticmethod
    def _play_out(ob, to_push, side):
        """Predicts the resulting outermost bid/ask after pushing the updates"""
        raise NotImplementedError("._play_out of l3 ob is not implemented yet")

    def is_subbed_to_l2_ob(self, symbol):
        return self.xs.has["orderbook"][
            "emulated"
        ] == "l3" and self.xs.is_subscribed_to(("orderbook", symbol))

    def assign_l2(self, symbol):
        if self.is_subbed_to_l2_ob(symbol) and self.is_synced[symbol]:
            amount_pcn = (
                self.xs.markets.get(symbol, {}).get("precision", {}).get("amount")
            )
            l2_ob = l3_to_l2(self.xs.l3_books[symbol], amount_pcn)
            # self.xs.ob_maintainer.send_orderbook(l2_ob)
            self.xs.create_orderbooks([l2_ob], enable_sub=True)
