import pytest
from unittest.mock import AsyncMock, MagicMock


def _make_binance_bot():
    from exchanges.binance import BinanceBot

    bot = BinanceBot.__new__(BinanceBot)
    bot.config = {"live": {"time_in_force": "post_only"}}
    bot.positions = {}
    bot.open_orders = {}
    bot.active_symbols = []
    bot.hedge_mode = True
    bot.binance_position_mode_hedged = True
    bot.log_once = MagicMock()
    bot.get_symbol_id_inv = lambda symbol: {
        "ETHUSDT": "ETH/USDT:USDT",
        "HYPEUSDT": "HYPE/USDT:USDT",
    }[symbol]
    return bot


def test_normalize_positions_infers_side_for_one_way_both_positions():
    bot = _make_binance_bot()

    positions = bot._normalize_positions(
        [
            {
                "symbol": "ETHUSDT",
                "positionSide": "BOTH",
                "positionAmt": "1.25",
                "entryPrice": "2000.0",
            },
            {
                "symbol": "HYPEUSDT",
                "positionSide": "BOTH",
                "positionAmt": "-3.5",
                "entryPrice": "40.0",
            },
        ]
    )

    assert positions == [
        {
            "symbol": "ETH/USDT:USDT",
            "position_side": "long",
            "size": 1.25,
            "price": 2000.0,
        },
        {
            "symbol": "HYPE/USDT:USDT",
            "position_side": "short",
            "size": -3.5,
            "price": 40.0,
        },
    ]


def test_get_position_side_for_order_infers_from_reduce_only_in_one_way_mode():
    bot = _make_binance_bot()

    open_long = {
        "symbol": "ETH/USDT:USDT",
        "side": "buy",
        "info": {"positionSide": "BOTH", "reduceOnly": False},
    }
    close_long = {
        "symbol": "ETH/USDT:USDT",
        "side": "sell",
        "info": {"positionSide": "BOTH", "reduceOnly": True},
    }

    assert bot._get_position_side_for_order(open_long) == "long"
    assert bot._get_position_side_for_order(close_long) == "long"


def test_build_order_params_uses_reduce_only_in_one_way_mode():
    bot = _make_binance_bot()
    bot.binance_position_mode_hedged = False

    params = bot._build_order_params(
        {
            "position_side": "long",
            "custom_id": "close_grid_long",
            "reduce_only": True,
            "type": "limit",
        }
    )

    assert params["newClientOrderId"] == "close_grid_long"
    assert params["reduceOnly"] is True
    assert "positionSide" not in params
    assert params["timeInForce"] == "GTX"


@pytest.mark.asyncio
async def test_update_exchange_config_detects_one_way_mode():
    bot = _make_binance_bot()
    bot.cca = MagicMock()
    bot.cca.fetch_position_mode = AsyncMock(return_value={"hedged": False, "info": {}})

    await bot.update_exchange_config()

    assert bot.hedge_mode is False
    assert bot.binance_position_mode_hedged is False
    bot.cca.fetch_position_mode.assert_called_once_with(params={"subType": "linear"})
    bot.log_once.assert_called_once()
