from __future__ import annotations

import asyncio
from datetime import datetime
from types import SimpleNamespace

import pytest

import telegram_read_model
from telegram_read_model import format_balance, format_positions_24h, format_status
from telegram_reports import (
    TelegramCredentials,
    TelegramReportManager,
    TelegramReportStateStore,
    build_report_message,
    due_report_windows,
    parse_telegram_credentials_text,
)


class FakePnlsManager:
    def __init__(self, events):
        self._events = list(events)

    def get_events(self, start_ms=None, end_ms=None, symbol=None):
        events = list(self._events)
        if start_ms is not None:
            events = [event for event in events if event.timestamp >= start_ms]
        if end_ms is not None:
            events = [event for event in events if event.timestamp <= end_ms]
        if symbol is not None:
            events = [event for event in events if event.symbol == symbol]
        return events


class FakeFillEvent(SimpleNamespace):
    def to_dict(self):
        return {
            "timestamp": self.timestamp,
            "symbol": self.symbol,
            "pnl": self.pnl,
            "fees": self.fees,
            "side": self.side,
            "position_side": self.position_side,
            "qty": self.qty,
            "price": self.price,
            "pb_order_type": self.pb_order_type,
        }


class FakeCM:
    def __init__(self, prices=None, errors=None):
        self.prices = prices or {}
        self.errors = errors or set()

    async def get_current_close(self, symbol, max_age_ms=None):
        if symbol in self.errors:
            raise RuntimeError(f"missing price for {symbol}")
        return self.prices[symbol]


class FakeCCA:
    def __init__(
        self,
        *,
        wallet_balances=None,
        wallet_balance_error=None,
        tickers=None,
        ticker_errors=None,
    ):
        self.wallet_balances = wallet_balances
        self.wallet_balance_error = wallet_balance_error
        self.tickers = tickers or {}
        self.ticker_errors = ticker_errors or {}

    async def sapi_get_asset_wallet_balance(self, params=None):
        if self.wallet_balance_error is not None:
            raise self.wallet_balance_error
        if self.wallet_balances is None:
            raise RuntimeError("wallet balance unavailable")
        return self.wallet_balances

    async def fetch_ticker(self, symbol):
        if symbol in self.ticker_errors:
            raise self.ticker_errors[symbol]
        if symbol not in self.tickers:
            raise RuntimeError(f"ticker unavailable for {symbol}")
        value = self.tickers[symbol]
        if isinstance(value, dict):
            return value
        return {"last": value}


class FakeBot:
    def __init__(
        self,
        *,
        events=None,
        positions=None,
        prices=None,
        price_errors=None,
        exchange="hyperliquid",
        wallet_balances=None,
        wallet_balance_error=None,
        tickers=None,
        ticker_errors=None,
        balance_raw=1000.0,
        balance_snap=None,
        user="test_user",
        fetch_balance_result=4321.0,
        fetch_balance_error=None,
        pb_modes=None,
        open_orders=None,
        c_mults=None,
    ):
        self.exchange = exchange
        self.user = user
        self.quote = "USDT"
        self.inverse = False
        self.c_mults = c_mults or {
            "BTC/USDT:USDT": 1.0,
            "ETH/USDT:USDT": 1.0,
            "HYPE/USDT:USDT": 1.0,
        }
        self.positions = positions or {}
        self.PB_modes = pb_modes or {}
        self.open_orders = open_orders or {}
        self.cm = FakeCM(prices=prices or {}, errors=price_errors or set())
        self.last_prices = dict(prices or {})
        self.cca = FakeCCA(
            wallet_balances=wallet_balances,
            wallet_balance_error=wallet_balance_error,
            tickers=tickers,
            ticker_errors=ticker_errors,
        )
        self._pnls_manager = FakePnlsManager(events or [])
        self.init_pnls_calls = 0
        self.update_pnls_calls = 0
        self.update_pnls_result = True
        self.balance_raw = balance_raw
        self.balance = balance_raw if balance_snap is None else balance_snap
        self.fetch_balance_result = fetch_balance_result
        self.fetch_balance_error = fetch_balance_error

    async def init_pnls(self):
        self.init_pnls_calls += 1

    async def update_pnls(self):
        self.update_pnls_calls += 1
        return self.update_pnls_result

    def get_raw_balance(self):
        return self.balance_raw

    def get_hysteresis_snapped_balance(self):
        return self.balance

    async def fetch_balance(self):
        if self.fetch_balance_error is not None:
            raise self.fetch_balance_error
        return self.fetch_balance_result


def make_event(
    *,
    ts_ms,
    symbol,
    pnl,
    fees,
    side,
    position_side,
    qty=0.0,
    price=0.0,
    pb_order_type="unknown",
):
    return FakeFillEvent(
        timestamp=ts_ms,
        symbol=symbol,
        pnl=pnl,
        fees=fees,
        side=side,
        position_side=position_side,
        qty=qty,
        price=price,
        pb_order_type=pb_order_type,
    )


def run(coro):
    return asyncio.run(coro)


BINANCE_WALLET_BALANCES_BTC = [
    {"walletName": "USDⓈ-M Futures", "balance": "0.1106067021665", "activate": True},
    {"walletName": "Earn", "balance": "0.0792825274786", "activate": True},
    {"walletName": "Copy Trading", "balance": "0.0194310076089", "activate": True},
    {"walletName": "Funding", "balance": "0.000124014", "activate": True},
]

BTC_USDT_TICKER = {"BTC/USDT": {"last": 100000.0}}


def test_parse_telegram_credentials_text_errors():
    with pytest.raises(ValueError, match="bot token"):
        parse_telegram_credentials_text("chatId: 123")
    with pytest.raises(ValueError, match="chatId"):
        parse_telegram_credentials_text("123456:abcdefghijklmnopqrstuvwxyz")


def test_due_report_windows_daily_and_weekly_ordering():
    now_local = datetime.fromisoformat("2026-04-12T22:05:00+08:00")
    windows = due_report_windows(now_local, {"daily": None, "weekly": None})
    assert [window.kind for window in windows] == ["daily", "weekly"]
    assert windows[0].start_ms < windows[0].end_ms
    assert windows[1].start_ms < windows[1].end_ms


def test_report_state_persists_across_restart(tmp_path):
    sent_messages = []

    async def fake_sender(credentials, text):
        sent_messages.append((credentials, text))
        return {"ok": True}

    manager = TelegramReportManager(
        credentials=TelegramCredentials(token="123456:abcdefghijklmnopqrstuvwxyz", chat_id="42"),
        state_store=TelegramReportStateStore(tmp_path / "state.json"),
        send_message_func=fake_sender,
    )
    bot = FakeBot(
        events=[
            make_event(
                ts_ms=int(datetime.fromisoformat("2026-04-07T15:00:00+00:00").timestamp() * 1000),
                symbol="BTC/USDT:USDT",
                pnl=12.5,
                fees={"cost": 0.5},
                side="sell",
                position_side="long",
            )
        ],
        positions={},
        prices={},
    )

    now_local = datetime.fromisoformat("2026-04-07T22:10:00+08:00")
    sent = run(manager.maybe_send_due_reports(bot, now_local=now_local))
    assert sent == ["daily", "weekly"]
    assert len(sent_messages) == 2

    manager_restarted = TelegramReportManager(
        credentials=TelegramCredentials(token="123456:abcdefghijklmnopqrstuvwxyz", chat_id="42"),
        state_store=TelegramReportStateStore(tmp_path / "state.json"),
        send_message_func=fake_sender,
    )
    sent_again = run(manager_restarted.maybe_send_due_reports(bot, now_local=now_local))
    assert sent_again == []
    assert len(sent_messages) == 2


def test_report_failure_does_not_mark_slot_sent(tmp_path):
    attempts = {"count": 0}

    async def flaky_sender(credentials, text):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("telegram down")
        return {"ok": True}

    state_path = tmp_path / "state.json"
    manager = TelegramReportManager(
        credentials=TelegramCredentials(token="123456:abcdefghijklmnopqrstuvwxyz", chat_id="42"),
        state_store=TelegramReportStateStore(state_path),
        send_message_func=flaky_sender,
    )
    bot = FakeBot(events=[], positions={}, prices={})
    now_local = datetime.fromisoformat("2026-04-07T22:10:00+08:00")

    with pytest.raises(RuntimeError, match="telegram down"):
        run(manager.maybe_send_due_reports(bot, now_local=now_local))
    assert not state_path.exists()

    sent = run(manager.maybe_send_due_reports(bot, now_local=now_local))
    assert sent == ["daily", "weekly"]
    assert attempts["count"] == 3


def test_build_report_message_handles_missing_current_price(caplog):
    caplog.set_level("WARNING")
    position_symbol = "BTC/USDT:USDT"
    bot = FakeBot(
        events=[
            make_event(
                ts_ms=int(datetime.fromisoformat("2026-04-07T12:30:00+00:00").timestamp() * 1000),
                symbol=position_symbol,
                pnl=0.0,
                fees=None,
                side="buy",
                position_side="long",
            )
        ],
        positions={
            position_symbol: {
                "long": {"size": 0.01, "price": 100000.0},
                "short": {"size": 0.0, "price": 0.0},
            }
        },
        prices={},
        price_errors={position_symbol},
    )
    window = due_report_windows(
        datetime.fromisoformat("2026-04-07T22:10:00+08:00"),
        {"daily": None, "weekly": "sent"},
    )[0]

    text = run(build_report_message(bot, window))

    assert "<b>当前持仓快照</b>" in text
    assert "现价=n/a" in text
    assert "uPnL=n/a" in text
    assert "部分持仓缺少现价，未实现盈亏为部分结果" in text
    assert "current price unavailable" in caplog.text


def test_build_report_message_includes_account_total_balance_for_binance():
    bot = FakeBot(
        exchange="binance",
        events=[],
        positions={},
        prices={},
        wallet_balances=BINANCE_WALLET_BALANCES_BTC,
        tickers=BTC_USDT_TICKER,
    )
    window = due_report_windows(
        datetime.fromisoformat("2026-04-07T22:10:00+08:00"),
        {"daily": None, "weekly": "sent"},
    )[0]

    text = run(build_report_message(bot, window))

    assert "<b>日报</b> | <code>binance:test_user</code>" in text
    assert "• 总资产：<code>20944.4251</code> USDT" in text
    assert "USDⓈ-M Futures" in text
    assert "<code>11060.6702</code> USDT（<code>52.81%</code>）" in text
    assert "• balance_raw：<code>1000</code> USDT" in text


def test_build_report_message_marks_account_distribution_unavailable_when_btc_price_missing():
    bot = FakeBot(
        exchange="binance",
        events=[],
        positions={},
        prices={},
        wallet_balances=BINANCE_WALLET_BALANCES_BTC,
        ticker_errors={"BTC/USDT": RuntimeError("ticker down")},
    )
    window = due_report_windows(
        datetime.fromisoformat("2026-04-07T22:10:00+08:00"),
        {"daily": None, "weekly": "sent"},
    )[0]

    text = run(build_report_message(bot, window))

    assert "• 总资产：<i>暂不可用</i>" in text
    assert "BTC 折算行情缺失" in text


def test_weekly_due_sends_daily_then_weekly(tmp_path):
    sent_texts = []

    async def fake_sender(credentials, text, parse_mode=None):
        sent_texts.append((text, parse_mode))
        return {"ok": True}

    manager = TelegramReportManager(
        credentials=TelegramCredentials(token="123456:abcdefghijklmnopqrstuvwxyz", chat_id="42"),
        state_store=TelegramReportStateStore(tmp_path / "state.json"),
        send_message_func=fake_sender,
    )
    bot = FakeBot(events=[], positions={}, prices={})
    now_local = datetime.fromisoformat("2026-04-12T22:05:00+08:00")

    sent = run(manager.maybe_send_due_reports(bot, now_local=now_local))

    assert sent == ["daily", "weekly"]
    assert len(sent_texts) == 2
    assert sent_texts[0][1] == "HTML"
    assert sent_texts[1][1] == "HTML"
    assert sent_texts[0][0].startswith("<b>日报</b>")
    assert sent_texts[1][0].startswith("<b>周报</b>")


def test_report_refresh_failure_raises(tmp_path):
    async def unused_sender(credentials, text, parse_mode=None):
        return {"ok": True}

    manager = TelegramReportManager(
        credentials=TelegramCredentials(token="123456:abcdefghijklmnopqrstuvwxyz", chat_id="42"),
        state_store=TelegramReportStateStore(tmp_path / "state.json"),
        send_message_func=unused_sender,
    )
    bot = FakeBot(events=[], positions={}, prices={})
    bot.update_pnls_result = False

    with pytest.raises(RuntimeError, match="failed to refresh fill events"):
        run(
            manager.maybe_send_due_reports(
                bot,
                now_local=datetime.fromisoformat("2026-04-07T22:10:00+08:00"),
            )
        )


def test_format_balance_includes_wallet_distribution_for_binance():
    bot = FakeBot(
        exchange="binance",
        positions={
            "BTC/USDT:USDT": {
                "long": {"size": 0.01, "price": 100000.0},
                "short": {"size": 0.0, "price": 0.0},
            }
        },
        prices={"BTC/USDT:USDT": 101000.0},
        wallet_balances=BINANCE_WALLET_BALANCES_BTC,
        tickers=BTC_USDT_TICKER,
        balance_raw=1000.0,
        balance_snap=995.0,
    )

    text = run(format_balance(bot))

    assert "<b>账户总览</b>" in text
    assert "总资产: <code>20944.43</code> USDT" in text
    assert "钱包分布" in text
    assert "USDⓈ-M Futures" in text
    assert "<code>11060.67</code> USDT (<code>+52.81%</code>)" in text
    assert "<b>交易账户快照</b>" in text
    assert "原始钱包：<code>1000.00</code> USDT" in text
    assert "平滑余额：<code>995.00</code> USDT" in text
    assert "账户权益：<code>1010.00</code> USDT" in text
    assert "未实现盈亏：<code>+10.00</code> USDT" in text


def test_format_balance_hides_snapped_when_unchanged():
    bot = FakeBot(
        exchange="binance",
        positions={},
        prices={},
        wallet_balances=BINANCE_WALLET_BALANCES_BTC,
        tickers=BTC_USDT_TICKER,
        balance_raw=1000.0,
        balance_snap=1000.0,
    )

    text = run(format_balance(bot))

    assert "平滑余额" not in text


def test_format_balance_falls_back_for_non_binance_without_distribution():
    bot = FakeBot(
        exchange="hyperliquid",
        positions={},
        prices={},
        fetch_balance_result=4321.5,
        balance_raw=1000.0,
        balance_snap=1000.0,
    )

    text = run(format_balance(bot))

    assert "账户余额: <code>4321.50</code> USDT" in text
    assert "钱包分布" not in text


def test_format_status_expands_position_summary_and_marks_partial_prices():
    bot = FakeBot(
        positions={
            "ETH/USDT:USDT": {
                "long": {"size": 4.454, "price": 2190.7046},
                "short": {"size": 0.0, "price": 0.0},
            },
            "BTC/USDT:USDT": {
                "long": {"size": 0.0, "price": 0.0},
                "short": {"size": -0.5, "price": 100000.0},
            },
        },
        prices={"ETH/USDT:USDT": 2200.69},
        pb_modes={
            "long": {"ETH/USDT:USDT": "normal"},
            "short": {"BTC/USDT:USDT": "graceful_stop"},
        },
        open_orders={
            "ETH/USDT:USDT": [
                {"order_type": "entry_grid_normal_long"},
                {"order_type": "close_grid_long"},
            ],
            "BTC/USDT:USDT": [{"order_type": "close_grid_short"}],
        },
        balance_raw=1000.0,
        balance_snap=1000.0,
    )

    text = format_status(bot)

    assert "<b>📊 Status</b>  (2 positions)" in text
    assert "Balance: <code>1000.00</code> USDT | uPnL: <code>+44.47</code> USDT | price partial" in text
    assert "🟢 <b>ETH</b> L | mode: <code>normal</code> | orders: <code>1E/1C</code>" in text
    assert (
        "size: <code>4.4540</code> | entry: <code>2190.7046</code> | "
        "mark: <code>2200.6900</code> | move: <code>+0.46%</code>"
    ) in text
    assert (
        "notional: <code>9801.87</code> USDT | uPnL: <code>+44.47</code> USDT | "
        "WE: <code>980.2%</code>"
    ) in text
    assert "🔴 <b>BTC</b> S | mode: <code>graceful_stop</code> | orders: <code>0E/1C</code>" in text
    assert "mark: <code>n/a</code> | move: <code>n/a</code>" in text
    assert "notional: <code>n/a</code> USDT | uPnL: <code>n/a</code> USDT | WE: <code>n/a</code>" in text


def test_format_positions_24h_shows_only_completed_position_cycles(monkeypatch):
    now_local = datetime.fromisoformat("2026-04-13T07:37:00+08:00")
    monkeypatch.setattr(telegram_read_model, "_utc_ms", lambda: int(now_local.timestamp() * 1000))

    bot = FakeBot(
        events=[
            make_event(
                ts_ms=int(datetime.fromisoformat("2026-04-12T09:43:00+08:00").timestamp() * 1000),
                symbol="ETH/USDT:USDT",
                pnl=0.0,
                fees=None,
                side="buy",
                position_side="long",
                qty=0.047,
                price=2219.95,
                pb_order_type="entry_grid_normal_long",
            ),
            make_event(
                ts_ms=int(datetime.fromisoformat("2026-04-12T18:54:00+08:00").timestamp() * 1000),
                symbol="ETH/USDT:USDT",
                pnl=0.0,
                fees=None,
                side="buy",
                position_side="long",
                qty=1.081,
                price=2198.87,
                pb_order_type="entry_grid_normal_long",
            ),
            make_event(
                ts_ms=int(datetime.fromisoformat("2026-04-12T23:31:00+08:00").timestamp() * 1000),
                symbol="ETH/USDT:USDT",
                pnl=0.0,
                fees=None,
                side="buy",
                position_side="long",
                qty=3.326,
                price=2174.66,
                pb_order_type="entry_grid_normal_long",
            ),
            make_event(
                ts_ms=int(datetime.fromisoformat("2026-04-13T01:51:00+08:00").timestamp() * 1000),
                symbol="ETH/USDT:USDT",
                pnl=44.47,
                fees=None,
                side="sell",
                position_side="long",
                qty=4.454,
                price=2200.69,
                pb_order_type="close_grid_long",
            ),
            make_event(
                ts_ms=int(datetime.fromisoformat("2026-04-13T00:10:00+08:00").timestamp() * 1000),
                symbol="HYPE/USDT:USDT",
                pnl=0.0,
                fees=None,
                side="buy",
                position_side="long",
                qty=10.0,
                price=40.5,
                pb_order_type="entry_grid_normal_long",
            ),
            make_event(
                ts_ms=int(datetime.fromisoformat("2026-04-13T00:20:00+08:00").timestamp() * 1000),
                symbol="HYPE/USDT:USDT",
                pnl=0.0,
                fees=None,
                side="buy",
                position_side="long",
                qty=15.0,
                price=40.8,
                pb_order_type="entry_grid_normal_long",
            ),
            make_event(
                ts_ms=int(datetime.fromisoformat("2026-04-13T00:37:00+08:00").timestamp() * 1000),
                symbol="HYPE/USDT:USDT",
                pnl=5.52,
                fees=None,
                side="sell",
                position_side="long",
                qty=20.0,
                price=41.169,
                pb_order_type="close_trailing_long",
            ),
            make_event(
                ts_ms=int(datetime.fromisoformat("2026-04-13T00:49:00+08:00").timestamp() * 1000),
                symbol="HYPE/USDT:USDT",
                pnl=0.25,
                fees=None,
                side="sell",
                position_side="long",
                qty=5.0,
                price=40.931,
                pb_order_type="close_trailing_long",
            ),
            make_event(
                ts_ms=int(datetime.fromisoformat("2026-04-13T06:00:00+08:00").timestamp() * 1000),
                symbol="BTC/USDT:USDT",
                pnl=0.0,
                fees=None,
                side="buy",
                position_side="long",
                qty=0.1,
                price=100000.0,
                pb_order_type="entry_grid_normal_long",
            ),
        ]
    )

    text = format_positions_24h(bot)

    assert "<b>🕘 过去24h已完成仓位</b>" in text
    assert "Positions: <code>2</code> | Symbols: <code>2</code> | Realized: <code>+50.24</code> USDT" in text
    assert "<b>ETH</b> L | realized: <code>+44.47</code> USDT | peak: <code>4.4540</code>" in text
    assert "open: <code>04-12 09:43</code> | close: <code>04-13 01:51</code>" in text
    assert "exit: <code>close_grid_long</code>" in text
    assert "<b>HYPE</b> L | realized: <code>+5.77</code> USDT | peak: <code>25.0000</code>" in text
    assert "exit: <code>close_trailing_long</code>" in text
    assert "BTC" not in text
    assert "entry |" not in text
    assert "... +" not in text


def test_format_positions_24h_ignores_open_position_activity(monkeypatch):
    now_local = datetime.fromisoformat("2026-04-13T07:37:00+08:00")
    monkeypatch.setattr(telegram_read_model, "_utc_ms", lambda: int(now_local.timestamp() * 1000))

    bot = FakeBot(
        events=[
            make_event(
                ts_ms=int(datetime.fromisoformat("2026-04-13T06:00:00+08:00").timestamp() * 1000),
                symbol="BTC/USDT:USDT",
                pnl=0.0,
                fees=None,
                side="buy",
                position_side="long",
                qty=0.1,
                price=100000.0,
                pb_order_type="entry_grid_normal_long",
            )
        ]
    )

    text = format_positions_24h(bot)

    assert "No completed positions in the past 24h" in text
