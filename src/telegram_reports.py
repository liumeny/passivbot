from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from html import escape as html_escape
from pathlib import Path
from typing import Any, Awaitable, Callable, Protocol, Sequence
from zoneinfo import ZoneInfo

import aiohttp
import passivbot_rust as pbr
from telegram_account_snapshot import build_account_balance_distribution_snapshot


logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
REPORT_HOUR = 22
REPORT_MINUTE = 0
TELEGRAM_API_BASE = "https://api.telegram.org"


class BotForTelegramReports(Protocol):
    exchange: str
    user: str
    quote: str
    inverse: bool
    c_mults: dict[str, float]
    positions: dict[str, dict[str, dict[str, float]]]
    cm: Any
    cca: Any
    _pnls_manager: Any

    async def init_pnls(self) -> None: ...

    async def update_pnls(self) -> bool: ...

    def get_raw_balance(self) -> float: ...


@dataclass(frozen=True)
class TelegramCredentials:
    token: str
    chat_id: str


@dataclass(frozen=True)
class ReportWindow:
    kind: str
    label: str
    slot_end_local: datetime
    start_local: datetime
    start_ms: int
    end_ms: int
    slot_key: str


def parse_telegram_credentials_text(text: str) -> TelegramCredentials:
    token_match = re.search(r"(?m)^\s*(\d{6,}:[A-Za-z0-9_-]{20,})\s*$", text)
    chat_id_match = re.search(r"(?im)^\s*chatid\s*:\s*(.+?)\s*$", text)
    if token_match is None:
        raise ValueError("Telegram credentials file is missing a bot token line")
    if chat_id_match is None:
        raise ValueError("Telegram credentials file is missing a chatId line")
    token = token_match.group(1).strip()
    chat_id = chat_id_match.group(1).strip()
    if not token:
        raise ValueError("Telegram bot token is empty")
    if not chat_id:
        raise ValueError("Telegram chatId is empty")
    return TelegramCredentials(token=token, chat_id=chat_id)


def load_telegram_credentials(path: str | Path) -> TelegramCredentials:
    path_obj = Path(path)
    if not path_obj.exists():
        raise FileNotFoundError(f"Telegram credentials file not found: {path_obj}")
    try:
        text = path_obj.read_text(encoding="utf-8")
    except Exception as exc:
        raise RuntimeError(f"Failed to read Telegram credentials file {path_obj}: {exc}") from exc
    try:
        return parse_telegram_credentials_text(text)
    except Exception as exc:
        raise ValueError(f"Failed to parse Telegram credentials file {path_obj}: {exc}") from exc


def _daily_slot_end_at_or_before(now_local: datetime) -> datetime:
    slot = now_local.replace(
        hour=REPORT_HOUR,
        minute=REPORT_MINUTE,
        second=0,
        microsecond=0,
    )
    if now_local < slot:
        slot -= timedelta(days=1)
    return slot


def _weekly_slot_end_at_or_before(now_local: datetime) -> datetime:
    days_since_sunday = (now_local.weekday() + 1) % 7
    slot = now_local.replace(
        hour=REPORT_HOUR,
        minute=REPORT_MINUTE,
        second=0,
        microsecond=0,
    ) - timedelta(days=days_since_sunday)
    if now_local < slot:
        slot -= timedelta(days=7)
    return slot


def build_daily_window(now_local: datetime) -> ReportWindow:
    slot_end = _daily_slot_end_at_or_before(now_local.astimezone(BEIJING_TZ))
    start_local = slot_end - timedelta(hours=24)
    return ReportWindow(
        kind="daily",
        label="Daily Report",
        slot_end_local=slot_end,
        start_local=start_local,
        start_ms=int(start_local.timestamp() * 1000),
        end_ms=int(slot_end.timestamp() * 1000),
        slot_key=slot_end.isoformat(),
    )


def build_weekly_window(now_local: datetime) -> ReportWindow:
    slot_end = _weekly_slot_end_at_or_before(now_local.astimezone(BEIJING_TZ))
    start_local = slot_end - timedelta(days=7)
    return ReportWindow(
        kind="weekly",
        label="Weekly Report",
        slot_end_local=slot_end,
        start_local=start_local,
        start_ms=int(start_local.timestamp() * 1000),
        end_ms=int(slot_end.timestamp() * 1000),
        slot_key=slot_end.isoformat(),
    )


def due_report_windows(
    now_local: datetime,
    state: dict[str, str | None],
) -> list[ReportWindow]:
    windows: list[ReportWindow] = []
    daily = build_daily_window(now_local)
    if state.get("daily") != daily.slot_key:
        windows.append(daily)
    weekly = build_weekly_window(now_local)
    if state.get("weekly") != weekly.slot_key:
        windows.append(weekly)
    if len(windows) == 2:
        windows.sort(key=lambda window: 0 if window.kind == "daily" else 1)
    return windows


def _format_timestamp_local(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=BEIJING_TZ).strftime("%Y-%m-%d %H:%M")


def _format_decimal(value: float | None, *, decimals: int = 4) -> str:
    if value is None:
        return "n/a"
    try:
        value = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if not math.isfinite(value):
        return "n/a"
    formatted = f"{value:.{decimals}f}"
    if "." in formatted:
        formatted = formatted.rstrip("0").rstrip(".")
    return formatted if formatted else "0"


def _format_signed(value: float | None, *, decimals: int = 4) -> str:
    if value is None:
        return "n/a"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if not math.isfinite(numeric):
        return "n/a"
    sign = "+" if numeric >= 0 else ""
    return f"{sign}{_format_decimal(numeric, decimals=decimals)}"


def _escape_html(value: Any) -> str:
    return html_escape(str(value), quote=False)


def _hbold(value: Any) -> str:
    return f"<b>{_escape_html(value)}</b>"


def _hmono(value: Any) -> str:
    return f"<code>{_escape_html(value)}</code>"


def _normalize_fee_entries(fees: Any) -> tuple[list[dict[str, Any]], bool]:
    if fees is None:
        return [], False
    if isinstance(fees, dict):
        return [fees], True
    if isinstance(fees, Sequence) and not isinstance(fees, (str, bytes, bytearray)):
        source = list(fees)
        entries = [entry for entry in source if isinstance(entry, dict)]
        return entries, len(entries) == len(source)
    return [], False


def summarize_fees(events: Sequence[Any]) -> tuple[float, bool]:
    total_fees = 0.0
    partial = False
    for event in events:
        entries, structure_ok = _normalize_fee_entries(getattr(event, "fees", None))
        if not structure_ok or not entries:
            partial = True
            continue
        entry_parsed = False
        for entry in entries:
            if "cost" not in entry:
                partial = True
                continue
            try:
                total_fees += float(entry["cost"] or 0.0)
                entry_parsed = True
            except (TypeError, ValueError):
                partial = True
        if not entry_parsed:
            partial = True
    return total_fees, partial


def is_close_event(event: Any) -> bool:
    side = str(getattr(event, "side", "") or "").lower()
    position_side = str(getattr(event, "position_side", "") or "").lower()
    if side == "sell" and position_side == "long":
        return True
    if side == "buy" and position_side == "short":
        return True
    pnl = float(getattr(event, "pnl", 0.0) or 0.0)
    return abs(pnl) > 0.0


def summarize_events(events: Sequence[Any]) -> dict[str, Any]:
    net_pnl = float(sum(float(getattr(event, "pnl", 0.0) or 0.0) for event in events))
    total_fees, fees_partial = summarize_fees(events)
    per_symbol: dict[str, float] = {}
    wins = 0
    losses = 0
    flats = 0
    for event in events:
        symbol = str(getattr(event, "symbol", "") or "")
        pnl = float(getattr(event, "pnl", 0.0) or 0.0)
        per_symbol[symbol] = per_symbol.get(symbol, 0.0) + pnl
        if is_close_event(event):
            if pnl > 0.0:
                wins += 1
            elif pnl < 0.0:
                losses += 1
            else:
                flats += 1
    top_symbols = sorted(per_symbol.items(), key=lambda item: (-item[1], item[0]))[:5]
    return {
        "fills_count": len(events),
        "symbols_count": len(
            {
                str(getattr(event, "symbol", "") or "")
                for event in events
                if getattr(event, "symbol", None)
            }
        ),
        "net_pnl": net_pnl,
        "fees_total": total_fees,
        "fees_partial": fees_partial,
        "wins": wins,
        "losses": losses,
        "flats": flats,
        "top_symbols": top_symbols,
    }


def _calc_position_upnl(
    position_side: str,
    entry_price: float,
    last_price: float,
    qty: float,
    c_mult: float,
) -> float:
    if position_side == "short":
        return float(pbr.calc_pnl_short(entry_price, last_price, qty, c_mult))
    return float(pbr.calc_pnl_long(entry_price, last_price, qty, c_mult))


async def _fetch_current_prices(
    bot: BotForTelegramReports,
    symbols: Sequence[str],
) -> dict[str, float | None]:
    prices: dict[str, float | None] = {}
    tasks = {
        symbol: asyncio.create_task(bot.cm.get_current_close(symbol, max_age_ms=60_000))
        for symbol in symbols
    }
    for symbol, task in tasks.items():
        try:
            value = float(await task)
            if not math.isfinite(value) or value <= 0.0:
                raise ValueError(f"invalid current close {value}")
            prices[symbol] = value
        except Exception as exc:
            logger.warning("[telegram] current price unavailable for %s: %s", symbol, exc)
            prices[symbol] = None
    return prices


async def build_positions_snapshot(bot: BotForTelegramReports) -> dict[str, Any]:
    positions_raw = getattr(bot, "positions", {}) or {}
    position_rows: list[dict[str, Any]] = []
    for symbol, sides in positions_raw.items():
        if not isinstance(sides, dict):
            continue
        for position_side in ("long", "short"):
            side_data = sides.get(position_side, {})
            size = float(side_data.get("size", 0.0) or 0.0)
            if abs(size) <= 1e-12:
                continue
            entry_price = float(side_data.get("price", 0.0) or 0.0)
            position_rows.append(
                {
                    "symbol": symbol,
                    "position_side": position_side,
                    "size": size,
                    "entry_price": entry_price,
                }
            )

    balance_raw = float(bot.get_raw_balance())
    if not position_rows:
        return {
            "balance_raw": balance_raw,
            "positions_count": 0,
            "positions": [],
            "total_upnl": 0.0,
            "prices_partial": False,
        }

    prices = await _fetch_current_prices(bot, [row["symbol"] for row in position_rows])
    total_upnl = 0.0
    prices_partial = False
    for row in position_rows:
        symbol = row["symbol"]
        position_side = row["position_side"]
        size = float(row["size"])
        entry_price = float(row["entry_price"])
        c_mult = float(getattr(bot, "c_mults", {}).get(symbol, 1.0) or 1.0)
        we_pct = 0.0
        try:
            if balance_raw > 0.0 and entry_price > 0.0:
                cost = abs(float(pbr.qty_to_cost(size, entry_price, c_mult)))
                we_pct = cost / balance_raw * 100.0
        except Exception:
            we_pct = 0.0
        last_price = prices.get(symbol)
        row["last_price"] = last_price
        row["we_pct"] = we_pct
        row["size_abs"] = abs(size)
        if last_price is None:
            row["upnl"] = None
            prices_partial = True
            continue
        try:
            row["upnl"] = _calc_position_upnl(
                position_side=position_side,
                entry_price=entry_price,
                last_price=last_price,
                qty=size,
                c_mult=c_mult,
            )
            total_upnl += float(row["upnl"])
        except Exception as exc:
            logger.warning(
                "[telegram] failed to compute uPnL for %s %s: %s",
                symbol,
                position_side,
                exc,
            )
            row["upnl"] = None
            prices_partial = True

    position_rows.sort(
        key=lambda row: (-abs(float(row["we_pct"])), row["symbol"], row["position_side"])
    )
    return {
        "balance_raw": balance_raw,
        "positions_count": len(position_rows),
        "positions": position_rows,
        "total_upnl": None if prices_partial else total_upnl,
        "prices_partial": prices_partial,
    }


async def build_account_balance_snapshot(bot: BotForTelegramReports) -> dict[str, Any]:
    shared = await build_account_balance_distribution_snapshot(bot)
    return {
        "attempted": shared.attempted,
        "total_balance": shared.total_balance,
        "partial": shared.partial,
        "unavailable": shared.unavailable,
        "reason": shared.reason,
        "wallets": [
            {
                "wallet_name": entry.wallet_name,
                "amount_quote": entry.amount_quote,
                "pct_of_total": entry.pct_of_total,
            }
            for entry in shared.wallets
        ],
    }


def _describe_account_snapshot_issue(account_snapshot: dict[str, Any]) -> str | None:
    if account_snapshot.get("unavailable"):
        reason = account_snapshot.get("reason")
        if reason == "btc_quote_price_unavailable":
            return "Binance 总资产分布暂不可用：BTC 折算行情缺失"
        if reason == "wallet_balance_unavailable":
            return "Binance 钱包总览接口暂不可用"
        if reason == "wallet_balance_payload_invalid":
            return "Binance 钱包总览返回格式异常"
        return "Binance 总资产分布暂不可用"
    if account_snapshot.get("partial"):
        return "部分钱包条目解析失败，以下账户概览为可确认部分"
    return None


async def build_report_message(bot: BotForTelegramReports, window: ReportWindow) -> str:
    manager = getattr(bot, "_pnls_manager", None)
    if manager is None:
        raise RuntimeError("[telegram] FillEventsManager is not initialized")
    end_inclusive_ms = max(window.start_ms, window.end_ms - 1)
    events = manager.get_events(start_ms=window.start_ms, end_ms=end_inclusive_ms)
    summary = summarize_events(events)
    snapshot = await build_positions_snapshot(bot)
    account_snapshot = await build_account_balance_snapshot(bot)

    notes: list[str] = []
    if summary["fees_partial"]:
        notes.append("手续费字段不完整")
        logger.warning(
            "[telegram] fees_partial for %s:%s %s slot %s",
            bot.exchange,
            bot.user,
            window.kind,
            window.slot_key,
        )
    if snapshot["prices_partial"]:
        notes.append("部分持仓缺少现价，未实现盈亏为部分结果")
    account_note = _describe_account_snapshot_issue(account_snapshot)
    if account_note:
        notes.append(account_note)

    title = "日报" if window.kind == "daily" else "周报"
    close_outcomes = f"{summary['wins']}胜 / {summary['losses']}负 / {summary['flats']}平"
    lines = [
        f"{_hbold(title)} | {_hmono(f'{bot.exchange}:{bot.user}')}",
        (
            f"时间区间（BJT）：{_hmono(_format_timestamp_local(window.start_ms))} → "
            f"{_hmono(_format_timestamp_local(window.end_ms))}"
        ),
        "",
        _hbold("交易概览"),
        f"• 成交笔数：{_hmono(summary['fills_count'])}",
        f"• 交易币种：{_hmono(summary['symbols_count'])}",
        f"• 已实现收益：{_hmono(_format_signed(summary['net_pnl']))} {_escape_html(bot.quote)}",
        f"• 手续费：{_hmono(_format_decimal(summary['fees_total']))} {_escape_html(bot.quote)}",
        f"• 平仓结果：{_hmono(close_outcomes)}",
        "• 贡献最佳币种：",
    ]
    if summary["top_symbols"]:
        for index, (symbol, pnl) in enumerate(summary["top_symbols"], start=1):
            lines.append(
                f"{index}. {_hmono(symbol)} {_hmono(_format_signed(pnl))} {_escape_html(bot.quote)}"
            )
    else:
        lines.append("无")

    lines.extend(
        [
            "",
            _hbold("账户概览"),
        ]
    )
    if account_snapshot["total_balance"] is not None:
        lines.append(
            f"• 总资产：{_hmono(_format_decimal(account_snapshot['total_balance']))} {_escape_html(bot.quote)}"
        )
    elif account_snapshot["attempted"] and account_snapshot["unavailable"]:
        lines.append("• 总资产：<i>暂不可用</i>")
    if account_snapshot["wallets"]:
        lines.append("• 钱包分布：")
        for entry in account_snapshot["wallets"]:
            lines.append(
                "  - {wallet}：{amount} {quote}（{pct}）".format(
                    wallet=_hmono(entry["wallet_name"]),
                    amount=_hmono(_format_decimal(entry["amount_quote"])),
                    quote=_escape_html(bot.quote),
                    pct=_hmono(_format_decimal(entry["pct_of_total"], decimals=2) + "%"),
                )
            )
    lines.extend(
        [
            "",
            _hbold("当前持仓快照"),
            f"• balance_raw：{_hmono(_format_decimal(snapshot['balance_raw']))} {_escape_html(bot.quote)}",
            (
                "• 总未实现盈亏："
                f"{_hmono(_format_signed(snapshot['total_upnl']) if snapshot['total_upnl'] is not None else 'n/a')} "
                f"{_escape_html(bot.quote)}"
            ),
            f"• 持仓数量：{_hmono(snapshot['positions_count'])}",
        ]
    )
    if snapshot["positions"]:
        for row in snapshot["positions"]:
            lines.append(
                "• {symbol} | {position_side} | 数量={size} | 开仓={entry} | 现价={last} | "
                "uPnL={upnl} {quote} | WE={we_pct}".format(
                    symbol=_escape_html(row["symbol"]),
                    position_side=_escape_html(row["position_side"]),
                    size=_format_decimal(row["size_abs"], decimals=6),
                    entry=_format_decimal(row["entry_price"]),
                    last=_format_decimal(row["last_price"]),
                    upnl=_format_signed(row["upnl"]),
                    quote=_escape_html(bot.quote),
                    we_pct=_format_decimal(row["we_pct"], decimals=2) + "%",
                )
            )
    else:
        lines.append("• 当前无持仓")

    if notes:
        lines.extend(["", _hbold("附注")])
        for note in notes:
            lines.append(f"• {_escape_html(note)}")
    return "\n".join(lines)


async def send_telegram_message(
    credentials: TelegramCredentials,
    text: str,
    *,
    parse_mode: str = "HTML",
    timeout_seconds: float = 15.0,
) -> dict[str, Any]:
    url = f"{TELEGRAM_API_BASE}/bot{credentials.token}/sendMessage"
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    payload = {"chat_id": credentials.chat_id, "text": text, "parse_mode": parse_mode}
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, data=payload) as response:
            response_text = await response.text()
            if response.status != 200:
                raise RuntimeError(
                    f"Telegram API HTTP {response.status}: {response_text[:300]}"
                )
            try:
                data = json.loads(response_text)
            except json.JSONDecodeError as exc:
                raise RuntimeError("Telegram API returned non-JSON response") from exc
            if not data.get("ok"):
                description = data.get("description") or "unknown Telegram API error"
                raise RuntimeError(f"Telegram API rejected message: {description}")
            return data


class TelegramReportStateStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def load(self) -> dict[str, str | None]:
        if not self.path.exists():
            return {"daily": None, "weekly": None}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning(
                "[telegram] failed to load report state %s: %s; ignoring cached state",
                self.path,
                exc,
            )
            return {"daily": None, "weekly": None}
        return {
            "daily": raw.get("daily"),
            "weekly": raw.get("weekly"),
        }

    def save(self, state: dict[str, str | None]) -> None:
        payload = {"daily": state.get("daily"), "weekly": state.get("weekly")}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp_path.replace(self.path)


class TelegramReportManager:
    def __init__(
        self,
        *,
        credentials: TelegramCredentials,
        state_store: TelegramReportStateStore,
        send_message_func: Callable[..., Awaitable[dict[str, Any]]] = send_telegram_message,
    ):
        self.credentials = credentials
        self.state_store = state_store
        self._send_message = send_message_func
        self._send_message_accepts_parse_mode = True
        try:
            parameters = inspect.signature(send_message_func).parameters.values()
            self._send_message_accepts_parse_mode = any(
                parameter.kind == inspect.Parameter.VAR_KEYWORD or parameter.name == "parse_mode"
                for parameter in parameters
            )
        except (TypeError, ValueError):
            self._send_message_accepts_parse_mode = True

    @classmethod
    def from_paths(
        cls,
        *,
        credentials_path: str | Path,
        state_path: str | Path,
        send_message_func: Callable[..., Awaitable[dict[str, Any]]] = send_telegram_message,
    ) -> "TelegramReportManager":
        return cls(
            credentials=load_telegram_credentials(credentials_path),
            state_store=TelegramReportStateStore(state_path),
            send_message_func=send_message_func,
        )

    async def send_text(self, text: str) -> dict[str, Any]:
        if self._send_message_accepts_parse_mode:
            return await self._send_message(self.credentials, text, parse_mode="HTML")
        return await self._send_message(self.credentials, text)

    async def maybe_send_due_reports(
        self,
        bot: BotForTelegramReports,
        *,
        now_local: datetime | None = None,
    ) -> list[str]:
        current_local = (now_local or datetime.now(tz=BEIJING_TZ)).astimezone(BEIJING_TZ)
        state = self.state_store.load()
        windows = due_report_windows(current_local, state)
        if not windows:
            return []

        await bot.init_pnls()
        pnls_ok = await bot.update_pnls()
        if not pnls_ok:
            raise RuntimeError("[telegram] failed to refresh fill events before sending report")

        sent: list[str] = []
        for window in windows:
            if state.get(window.kind) == window.slot_key:
                continue
            text = await build_report_message(bot, window)
            await self.send_text(text)
            state[window.kind] = window.slot_key
            self.state_store.save(state)
            sent.append(window.kind)
        return sent
