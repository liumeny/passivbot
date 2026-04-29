import pytest
from unittest.mock import AsyncMock

from passivbot import Passivbot


class _WhitelistIsolationHarness(Passivbot):
    def __init__(self):
        self.config = {"live": {"max_ohlcv_fetches_per_minute": 0}}
        self.PB_modes = {"long": {}, "short": {}}
        self.PB_mode_stop = {"long": "graceful_stop", "short": "manual"}
        self.positions = {
            "AAVE/USDT:USDT": {
                "long": {"size": 3.0, "price": 100.0},
                "short": {"size": 0.0, "price": 0.0},
            },
            "APT/USDT:USDT": {
                "long": {"size": 10.0, "price": 1.0},
                "short": {"size": 0.0, "price": 0.0},
            },
        }
        self.open_orders = {"DOGE/USDT:USDT": [{"id": "manual-open-order"}]}
        self.active_symbols = []
        self.coin_overrides = {}
        self.ineligible_symbols = {}
        self.approved_coins_minus_ignored_coins = {
            "long": {"ETH/USDT:USDT", "HYPE/USDT:USDT"},
            "short": set(),
        }
        self.hedge_mode = False

    async def update_effective_min_cost(self):
        return

    def refresh_approved_ignored_coins_lists(self):
        return

    def set_wallet_exposure_limits(self):
        return

    def is_forager_mode(self, _pside):
        return False

    async def update_first_timestamps(self):
        return

    def get_forced_PB_mode(self, pside, symbol=None):
        return None

    async def get_filtered_coins(self, pside, max_network_fetches=None):
        if pside == "long":
            return ["ETH/USDT:USDT", "HYPE/USDT:USDT"]
        return []

    def get_max_n_positions(self, pside):
        return 2 if pside == "long" else 0

    def _log_mode_changes(self, res, previous_PB_modes):
        return


@pytest.mark.asyncio
async def test_execution_cycle_keeps_non_whitelisted_positions_manual_and_free_of_slots():
    bot = _WhitelistIsolationHarness()

    assert bot.get_current_n_positions("long") == 0
    assert bot.get_symbols_approved_or_has_pos("long") == {"ETH/USDT:USDT", "HYPE/USDT:USDT"}

    await bot.execution_cycle()

    assert bot.PB_modes["long"]["AAVE/USDT:USDT"] == "manual"
    assert bot.PB_modes["long"]["APT/USDT:USDT"] == "manual"
    assert bot.PB_modes["long"]["DOGE/USDT:USDT"] == "manual"
    assert bot.PB_modes["long"]["ETH/USDT:USDT"] == "normal"
    assert bot.PB_modes["long"]["HYPE/USDT:USDT"] == "normal"


@pytest.mark.asyncio
async def test_update_exchange_configs_skips_manual_only_symbols():
    bot = Passivbot.__new__(Passivbot)
    bot.active_symbols = ["AAVE/USDT:USDT", "ETH/USDT:USDT"]
    bot.already_updated_exchange_config_symbols = set()
    bot.PB_modes = {
        "long": {"AAVE/USDT:USDT": "manual", "ETH/USDT:USDT": "normal"},
        "short": {"AAVE/USDT:USDT": "manual", "ETH/USDT:USDT": "manual"},
    }
    bot.update_exchange_config_by_symbols = AsyncMock()

    await Passivbot.update_exchange_configs(bot)

    bot.update_exchange_config_by_symbols.assert_awaited_once_with(["ETH/USDT:USDT"])
    assert bot.already_updated_exchange_config_symbols == {"ETH/USDT:USDT"}
