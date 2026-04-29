"""TelegramReadModel — pure formatting functions for Telegram messages.

Each function takes the live Passivbot instance and returns a formatted
HTML string suitable for Telegram messages. No side-effects, no mutations.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone, timedelta
from html import escape as html_escape
from typing import Any, Dict, List, Optional

from telegram_account_snapshot import build_account_balance_distribution_snapshot

# Version — can be overridden by git tag or setup.py
PASSIVBOT_VERSION = "0.1.0"


# ── Helpers ─────────────────────────────────────────────────────────────

def _coin(symbol: str) -> str:
    """Extract coin name from symbol like 'ETH/USDT:USDT' → 'ETH'."""
    return symbol.split("/")[0] if "/" in symbol else symbol


def _f(val: float, decimals: int = 2) -> str:
    """Format a float with fixed decimals."""
    return f"{val:.{decimals}f}"


def _sign(val: float, decimals: int = 2) -> str:
    """Format float with explicit +/- sign."""
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:.{decimals}f}"


def _pct(val: float) -> str:
    """Format as percentage with sign."""
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:.2f}%"


def _escape(text: Any) -> str:
    return html_escape(str(text), quote=False)


def _mono(text: str) -> str:
    """Wrap text in monospace HTML tag."""
    return f"<code>{text}</code>"


def _bold(text: str) -> str:
    return f"<b>{text}</b>"


def _utc_ms() -> int:
    return int(time.time() * 1000)


def _format_duration(ms: int) -> str:
    """Format milliseconds as human-readable duration."""
    total_seconds = ms // 1000
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    if days > 0:
        return f"{days}d{hours}h{minutes}m"
    if hours > 0:
        return f"{hours}h{minutes}m"
    if minutes > 0:
        return f"{minutes}m{seconds}s"
    return f"{seconds}s"


def _truncate(text: str, max_len: int = 4000) -> str:
    """Truncate text to fit Telegram's 4096 char limit."""
    if len(text) <= max_len:
        return text
    return text[:max_len] + "\n\n<i>... (truncated)</i>"


def _fmt_local(ts_ms: int) -> str:
    """Format a timestamp in Asia/Shanghai for operator-facing output."""
    return datetime.fromtimestamp(
        ts_ms / 1000, tz=timezone(timedelta(hours=8))
    ).strftime("%m-%d %H:%M")


async def _fetch_account_total_balance(bot: Any) -> Optional[float]:
    """Fallback total balance for exchanges without wallet-distribution support."""
    fetch_balance = getattr(bot, "fetch_balance", None)
    if callable(fetch_balance):
        try:
            value = await fetch_balance()
            return float(value)
        except Exception as e:
            logging.warning("[telegram] unable to fetch account total balance: %s", e)
    return None


# ── Command Formatters ──────────────────────────────────────────────────

def format_help(read_only: bool = False) -> str:
    """Return the /help message listing all available commands."""
    lines = [
        _bold("📋 Passivbot Commands"),
        "",
        _bold("Status"),
        "/help — this message",
        "/health — uptime, loop, errors, memory",
        "/balance — total account balance + wallet snapshot",
        "/status — all positions summary",
        "/status &lt;coin&gt; — single coin detail",
        "/positions24h — completed positions closed over the past 24h",
        "/fills [n] — latest fills (default 10)",
        "/profit [24h|7d|30d] — PnL summary",
        "/daily [n] — daily PnL (default 7)",
        "/version — bot version",
        "/logs [n] — tail recent logs (default 20)",
    ]
    if not read_only:
        lines += [
            "",
            _bold("Controls"),
            "/graceful_stop &lt;long|short&gt; — side-wide graceful stop",
            "/restart — restart bot",
        ]
    return "\n".join(lines)


def format_health(bot: Any) -> str:
    """Format the /health response from bot runtime state."""
    now_ms = _utc_ms()
    uptime_ms = now_ms - getattr(bot, "_health_start_ms", now_ms)
    uptime_str = _format_duration(uptime_ms)

    loop_ms = getattr(bot, "_last_loop_duration_ms", 0)
    loop_str = f"{loop_ms / 1000:.1f}s" if loop_ms > 0 else "n/a"

    fills = getattr(bot, "_health_fills", 0)
    pnl = getattr(bot, "_health_pnl", 0.0)
    errors = getattr(bot, "_health_errors", 0)
    rate_limits = getattr(bot, "_health_rate_limits", 0)
    ws_reconnects = getattr(bot, "_health_ws_reconnects", 0)
    orders_placed = getattr(bot, "_health_orders_placed", 0)
    orders_cancelled = getattr(bot, "_health_orders_cancelled", 0)

    # Memory
    mem_str = "n/a"
    try:
        import resource as res_mod
        rss_mb = res_mod.getrusage(res_mod.RUSAGE_SELF).ru_maxrss / 1024 / 1024
        mem_str = f"{rss_mb:.0f} MB"
    except Exception:
        pass

    # Count positions
    n_long = n_short = 0
    positions = getattr(bot, "positions", {})
    for sym, pos_data in positions.items():
        if pos_data.get("long", {}).get("size", 0.0) != 0.0:
            n_long += 1
        if pos_data.get("short", {}).get("size", 0.0) != 0.0:
            n_short += 1

    lines = [
        _bold("🏥 Health"),
        "",
        f"⏱ Uptime: {_mono(uptime_str)}",
        f"🔄 Loop: {_mono(loop_str)}",
        f"📊 Positions: {_mono(f'{n_long}L / {n_short}S')}",
        f"📝 Orders: {_mono(f'+{orders_placed} / -{orders_cancelled}')}",
        f"💰 Fills: {_mono(str(fills))} (PnL: {_mono(_sign(pnl))})",
        f"❌ Errors: {_mono(str(errors))}",
        f"⚡ Rate limits: {_mono(str(rate_limits))}",
        f"🔌 WS reconnects: {_mono(str(ws_reconnects))}",
        f"💾 Memory: {_mono(mem_str)}",
    ]
    return "\n".join(lines)


def format_status(bot: Any) -> str:
    """Format the /status response — summary of all open positions."""
    positions = getattr(bot, "positions", {})
    pb_modes = getattr(bot, "PB_modes", {})
    last_prices = getattr(bot, "last_prices", {})
    open_orders = getattr(bot, "open_orders", {})
    balance = float(getattr(bot, "balance", 1e-12) or 1e-12)
    quote = getattr(bot, "quote", "USDT")

    # Collect positions with non-zero size
    entries = []
    for symbol, pos_data in positions.items():
        c_mult = float(getattr(bot, "c_mults", {}).get(symbol, 1.0))
        for pside in ("long", "short"):
            side_data = pos_data.get(pside, {})
            size = float(side_data.get("size", 0.0))
            if size == 0.0:
                continue
            price = float(side_data.get("price", 0.0))
            current_price = float(last_prices.get(symbol, 0.0) or 0.0)

            # PnL calculation
            upnl = None
            price_move_pct = None
            notional = None
            we = None
            if price > 0 and current_price > 0:
                if pside == "long":
                    upnl = (current_price - price) * abs(size) * c_mult
                    price_move_pct = (current_price / price - 1.0) * 100.0
                else:
                    upnl = (price - current_price) * abs(size) * c_mult
                    price_move_pct = (price - current_price) / price * 100.0
                notional = abs(size) * current_price * c_mult
                we = notional / max(balance, 1e-12) * 100.0

            mode = pb_modes.get(pside, {}).get(symbol, "?")

            # Open order counts for this symbol/side
            n_entry_orders = 0
            n_close_orders = 0
            n_other_orders = 0
            for o in open_orders.get(symbol, []):
                otype = o.get("order_type", "") if isinstance(o, dict) else ""
                if pside in str(otype).lower():
                    otype_l = str(otype).lower()
                    if "entry" in otype_l:
                        n_entry_orders += 1
                    elif "close" in otype_l:
                        n_close_orders += 1
                    else:
                        n_other_orders += 1

            entries.append({
                "coin": _coin(symbol),
                "symbol": symbol,
                "pside": pside,
                "size": size,
                "price": price,
                "current": current_price,
                "upnl": upnl,
                "price_move_pct": price_move_pct,
                "notional": notional,
                "we": we,
                "mode": mode,
                "entry_orders": n_entry_orders,
                "close_orders": n_close_orders,
                "other_orders": n_other_orders,
            })

    if not entries:
        balance_raw = 0.0
        try:
            balance_raw = bot.get_raw_balance()
        except Exception:
            pass
        return (
            _bold("📊 Status") + "\n\n"
            f"No open positions\n"
            f"Balance: {_mono(_f(balance_raw))} {quote}"
        )

    # Sort by coin then side
    entries.sort(key=lambda e: (e["coin"], e["pside"]))

    # Build summary
    balance_raw = 0.0
    try:
        balance_raw = bot.get_raw_balance()
    except Exception:
        pass
    total_upnl = sum(float(e["upnl"] or 0.0) for e in entries)
    partial_upnl = any(e["upnl"] is None for e in entries)

    lines = [
        _bold("📊 Status") + f"  ({len(entries)} positions)",
        f"Balance: {_mono(_f(balance_raw))} {quote} | "
        f"uPnL: {_mono(_sign(total_upnl))} {quote}"
        + (" | price partial" if partial_upnl else ""),
        "",
    ]

    for e in entries:
        side_emoji = "🟢" if e["pside"] == "long" else "🔴"
        mark_str = _mono(_f(e["current"], 4)) if e["current"] > 0 else _mono("n/a")
        move_str = (
            _mono(_sign(e["price_move_pct"]) + "%")
            if e["price_move_pct"] is not None
            else _mono("n/a")
        )
        notional_str = _mono(_f(e["notional"])) if e["notional"] is not None else _mono("n/a")
        pnl_str = _mono(_sign(e["upnl"])) if e["upnl"] is not None else _mono("n/a")
        we_str = _mono(_f(e["we"], 1) + "%") if e["we"] is not None else _mono("n/a")
        order_counts = f"{e['entry_orders']}E/{e['close_orders']}C"
        if e["other_orders"]:
            order_counts += f"/{e['other_orders']}O"
        lines.append(
            f"{side_emoji} {_bold(e['coin'])} {e['pside'][0].upper()} | "
            f"mode: {_mono(e['mode'])} | "
            f"orders: {_mono(order_counts)}"
        )
        lines.append(
            f"  size: {_mono(_f(abs(e['size']), 4))} | "
            f"entry: {_mono(_f(e['price'], 4))} | "
            f"mark: {mark_str} | "
            f"move: {move_str}"
        )
        lines.append(
            f"  notional: {notional_str} {quote} | "
            f"uPnL: {pnl_str} {quote} | "
            f"WE: {we_str}"
        )
        lines.append("")

    return _truncate("\n".join(lines).rstrip())


def format_status_detail(bot: Any, coin: str) -> str:
    """Format detailed /status <coin> view."""
    # Find the symbol for this coin
    symbol = None
    positions = getattr(bot, "positions", {})
    for sym in positions:
        if _coin(sym).upper() == coin.upper():
            symbol = sym
            break

    if not symbol:
        # Try all known symbols
        markets = getattr(bot, "markets_dict", {})
        for sym in markets:
            if _coin(sym).upper() == coin.upper():
                symbol = sym
                break

    if not symbol:
        return f"❌ Coin {_bold(coin.upper())} not found"

    pos_data = positions.get(symbol, {})
    last_prices = getattr(bot, "last_prices", {})
    current_price = float(last_prices.get(symbol, 0.0))
    pb_modes = getattr(bot, "PB_modes", {})
    open_orders = getattr(bot, "open_orders", {})
    c_mult = float(getattr(bot, "c_mults", {}).get(symbol, 1.0))
    balance = float(getattr(bot, "balance", 1e-12))
    quote = getattr(bot, "quote", "USDT")

    lines = [
        _bold(f"📊 {_coin(symbol)}") + f"  |  {_mono(_f(current_price, 4))} {quote}",
        "",
    ]

    for pside in ("long", "short"):
        side_data = pos_data.get(pside, {})
        size = float(side_data.get("size", 0.0))
        price = float(side_data.get("price", 0.0))
        mode = pb_modes.get(pside, {}).get(symbol, "?")
        side_emoji = "🟢" if pside == "long" else "🔴"

        lines.append(f"{side_emoji} {_bold(pside.upper())}")
        lines.append(f"  Mode: {_mono(mode)}")

        if size == 0.0:
            lines.append("  No position")
        else:
            upnl = 0.0
            if price > 0 and current_price > 0:
                if pside == "long":
                    upnl = (current_price - price) * abs(size) * c_mult
                else:
                    upnl = (price - current_price) * abs(size) * c_mult

            we = abs(size) * current_price * c_mult / max(balance, 1e-12) if current_price > 0 else 0.0

            lines.append(f"  Size: {_mono(_f(abs(size), 6))}")
            lines.append(f"  Entry: {_mono(_f(price, 4))}")
            lines.append(f"  uPnL: {_mono(_sign(upnl))} {quote}")
            lines.append(f"  WE: {_mono(_f(we * 100, 1))}%")

        # Orders for this side
        side_orders = []
        for o in open_orders.get(symbol, []):
            if isinstance(o, dict):
                otype = str(o.get("order_type", ""))
                if pside in otype.lower():
                    oprice = o.get("price", 0.0)
                    oqty = o.get("qty", 0.0)
                    side_orders.append(f"    {otype}: {_f(float(oqty), 6)} @ {_f(float(oprice), 4)}")

        if side_orders:
            lines.append(f"  Orders ({len(side_orders)}):")
            lines.extend(side_orders[:10])  # Max 10 orders per side
            if len(side_orders) > 10:
                lines.append(f"    ... +{len(side_orders) - 10} more")

        lines.append("")

    # Recent fills for this symbol
    fills = _get_recent_fills_for_symbol(bot, symbol, n=5)
    if fills:
        lines.append(_bold("Recent Fills"))
        for f in fills:
            ts = datetime.fromtimestamp(f["timestamp"] / 1000, tz=timezone.utc).strftime("%m-%d %H:%M")
            lines.append(
                f"  {ts} | {f['pside'][0].upper()} | {f['type']} | "
                f"qty: {_f(f['qty'], 6)} @ {_f(f['price'], 4)} | "
                f"pnl: {_sign(f['pnl'])}"
            )

    return _truncate("\n".join(lines))


async def format_balance(bot: Any) -> str:
    """Format the /balance response using a fresh account-balance API query when available."""
    try:
        balance_raw = bot.get_raw_balance()
        balance_snap = bot.get_hysteresis_snapped_balance()
    except Exception:
        balance_raw = float(getattr(bot, "balance_raw", 0.0))
        balance_snap = float(getattr(bot, "balance", 0.0))
    quote = getattr(bot, "quote", "USDT")
    account_snapshot = await build_account_balance_distribution_snapshot(bot)
    fallback_account_total = None
    if not account_snapshot.attempted:
        fallback_account_total = await _fetch_account_total_balance(bot)

    lines = [
        _bold("💰 账户余额"),
        "",
        _bold("账户总览"),
    ]
    if account_snapshot.total_balance is not None:
        lines.append(f"总资产: {_mono(_f(account_snapshot.total_balance))} {quote}")
    elif account_snapshot.unavailable:
        lines.append(
            "<i>Binance 总资产分布暂不可用，当前无法可靠折算为统一 "
            f"{_escape(quote)} 金额。</i>"
        )
    elif fallback_account_total is not None:
        lines.append(f"账户余额: {_mono(_f(fallback_account_total))} {quote}")
    else:
        lines.append("<i>账户总览暂不可用</i>")

    if account_snapshot.wallets:
        lines.extend(
            [
                "",
                _bold("钱包分布"),
            ]
        )
        for entry in account_snapshot.wallets:
            lines.append(
                "• {wallet}: {amount} {quote} ({pct})".format(
                    wallet=_escape(entry.wallet_name),
                    amount=_mono(_f(entry.amount_quote)),
                    quote=_escape(quote),
                    pct=_mono(_pct(entry.pct_of_total)),
                )
            )
        if account_snapshot.partial:
            lines.append("<i>部分钱包条目解析失败，以上为可确认部分。</i>")

    lines.extend(
        [
            "",
            _bold("交易账户快照"),
            f"原始钱包：{_mono(_f(balance_raw))} {quote}",
        ]
    )

    if abs(balance_raw - balance_snap) > 0.01:
        lines.append(f"平滑余额：{_mono(_f(balance_snap))} {quote}")

    # equity = balance + unrealized PnL
    positions = getattr(bot, "positions", {})
    last_prices = getattr(bot, "last_prices", {})
    total_upnl = 0.0
    for symbol, pos_data in positions.items():
        c_mult = float(getattr(bot, "c_mults", {}).get(symbol, 1.0))
        current_price = float(last_prices.get(symbol, 0.0))
        for pside in ("long", "short"):
            size = float(pos_data.get(pside, {}).get("size", 0.0))
            price = float(pos_data.get(pside, {}).get("price", 0.0))
            if size != 0.0 and price > 0 and current_price > 0:
                if pside == "long":
                    total_upnl += (current_price - price) * abs(size) * c_mult
                else:
                    total_upnl += (price - current_price) * abs(size) * c_mult

    equity = balance_raw + total_upnl
    lines.append(f"账户权益：{_mono(_f(equity))} {quote}")
    lines.append(f"未实现盈亏：{_mono(_sign(total_upnl))} {quote}")

    return "\n".join(lines)


def format_positions_24h(bot: Any) -> str:
    """Format fully closed past-24h position cycles using fill history."""
    now_ms = _utc_ms()
    cutoff = now_ms - 24 * 3600 * 1000
    fills = _get_all_fills(bot)
    quote = getattr(bot, "quote", "USDT")

    if not fills:
        return (
            _bold("🕘 过去24h已完成仓位")
            + "\n\n"
            + f"Window: {_mono(_fmt_local(cutoff))} → {_mono(_fmt_local(now_ms))}\n"
            + "No completed positions in the past 24h"
        )

    completed = [
        cycle
        for cycle in _extract_completed_position_cycles(fills)
        if cutoff <= int(cycle["closed_at"]) <= now_ms
    ]

    if not completed:
        return (
            _bold("🕘 过去24h已完成仓位")
            + "\n\n"
            + f"Window: {_mono(_fmt_local(cutoff))} → {_mono(_fmt_local(now_ms))}\n"
            + "No completed positions in the past 24h"
        )

    completed.sort(key=lambda row: (-int(row["closed_at"]), row["coin"], row["pside"]))
    total_realized = sum(float(row["realized"]) for row in completed)
    symbols_count = len({row["symbol"] for row in completed})

    lines = [
        _bold("🕘 过去24h已完成仓位"),
        "",
        f"Window: {_mono(_fmt_local(cutoff))} → {_mono(_fmt_local(now_ms))}",
        f"Positions: {_mono(str(len(completed)))} | Symbols: {_mono(str(symbols_count))} | Realized: {_mono(_sign(total_realized))} {quote}",
        "",
    ]

    for cycle in completed:
        side_label = "L" if cycle["pside"] == "long" else "S"
        duration_ms = (
            int(cycle["closed_at"]) - int(cycle["opened_at"])
            if cycle["opened_at"] is not None
            else 0
        )
        open_time = (
            _mono(_fmt_local(int(cycle["opened_at"])))
            if cycle["opened_at"] is not None
            else _mono("n/a")
        )
        avg_entry = (
            _mono(_f(float(cycle["avg_entry"]), 4))
            if cycle["avg_entry"] is not None
            else _mono("n/a")
        )
        avg_close = (
            _mono(_f(float(cycle["avg_close"]), 4))
            if cycle["avg_close"] is not None
            else _mono("n/a")
        )
        close_type = _mono(str(cycle["close_type"] or "n/a"))
        lines.append(
            f"{_bold(cycle['coin'])} {side_label} | "
            f"realized: {_mono(_sign(float(cycle['realized'])))} {quote} | "
            f"peak: {_mono(_f(float(cycle['peak_qty']), 4))} | "
            f"dur: {_mono(_format_duration(max(duration_ms, 0)))}"
        )
        lines.append(
            f"  open: {open_time} | "
            f"close: {_mono(_fmt_local(int(cycle['closed_at'])))} | "
            f"avg: {avg_entry} → {avg_close} | "
            f"exit: {close_type}"
        )
        lines.append("")

    return _truncate("\n".join(lines).rstrip())


def format_fills(bot: Any, n: int = 10) -> str:
    """Format the /fills response showing the latest N fill events."""
    fills = _get_recent_fills(bot, n)
    if not fills:
        return _bold("📝 Fills") + "\n\nNo fill events"

    lines = [_bold(f"📝 Latest {len(fills)} Fills"), ""]

    for f in fills:
        ts = datetime.fromtimestamp(
            f["timestamp"] / 1000, tz=timezone.utc
        ).strftime("%m-%d %H:%M")
        coin = _coin(f.get("symbol", "?"))
        pside = f.get("pside", "?")[0].upper()
        otype = f.get("type", "?")
        # Shorten order type
        if len(otype) > 20:
            otype = otype[:20]
        pnl_str = _sign(f["pnl"]) if f["pnl"] != 0.0 else "-"
        lines.append(
            f"{_mono(ts)} | {coin} {pside} | {otype} | "
            f"qty: {_f(f['qty'], 4)} @ {_f(f['price'], 4)} | pnl: {pnl_str}"
        )

    return _truncate("\n".join(lines))


def format_profit(bot: Any, window: Optional[str] = None) -> str:
    """Format the /profit response with realized/unrealized PnL."""
    fills = _get_all_fills(bot)
    now_ms = _utc_ms()

    # Compute realized PnL for various windows
    windows = {
        "24h": 24 * 3600 * 1000,
        "7d": 7 * 24 * 3600 * 1000,
        "30d": 30 * 24 * 3600 * 1000,
    }

    realized = {}
    fill_counts = {}
    for label, period_ms in windows.items():
        cutoff = now_ms - period_ms
        period_fills = [f for f in fills if f["timestamp"] >= cutoff]
        realized[label] = sum(f["pnl"] for f in period_fills)
        fill_counts[label] = len(period_fills)

    realized["all"] = sum(f["pnl"] for f in fills)
    fill_counts["all"] = len(fills)

    # Unrealized PnL
    total_upnl = _calc_total_unrealized_pnl(bot)
    quote = getattr(bot, "quote", "USDT")

    lines = [
        _bold("💹 Profit Summary"),
        "",
        f"Unrealized: {_mono(_sign(total_upnl))} {quote}",
        "",
        _bold("Realized PnL"),
        f"  24h:     {_mono(_sign(realized['24h']))} ({fill_counts['24h']} fills)",
        f"  7d:      {_mono(_sign(realized['7d']))} ({fill_counts['7d']} fills)",
        f"  30d:     {_mono(_sign(realized['30d']))} ({fill_counts['30d']} fills)",
        f"  All:     {_mono(_sign(realized['all']))} ({fill_counts['all']} fills)",
    ]

    return "\n".join(lines)


def format_daily(bot: Any, n: int = 7) -> str:
    """Format the /daily response with daily realized PnL buckets."""
    fills = _get_all_fills(bot)
    if not fills:
        return _bold("📅 Daily PnL") + "\n\nNo fill history"

    # Bucket fills by day (UTC)
    day_pnl: Dict[str, float] = {}
    day_count: Dict[str, int] = {}
    for f in fills:
        day = datetime.fromtimestamp(
            f["timestamp"] / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d")
        day_pnl[day] = day_pnl.get(day, 0.0) + f["pnl"]
        day_count[day] = day_count.get(day, 0) + 1

    # Get the last n days
    today = datetime.now(timezone.utc).date()
    days = []
    for i in range(n):
        day = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        days.append(day)
    days.reverse()

    lines = [_bold(f"📅 Daily PnL (last {n} days)"), ""]
    total = 0.0
    for day in days:
        pnl = day_pnl.get(day, 0.0)
        count = day_count.get(day, 0)
        total += pnl
        bar = "🟩" if pnl > 0 else ("🟥" if pnl < 0 else "⬜")
        lines.append(f"{_mono(day)} | {bar} {_mono(_sign(pnl)):>12} | {count} fills")

    quote = getattr(bot, "quote", "USDT")
    lines.append("")
    lines.append(f"Total: {_mono(_sign(total))} {quote}")

    return "\n".join(lines)


def format_version() -> str:
    """Format the /version response."""
    return (
        _bold("🤖 Passivbot") + f" v{PASSIVBOT_VERSION}\n"
        f"Telegram integration enabled"
    )


def format_logs(log_dir: Optional[str] = None, n: int = 20) -> str:
    """Format the /logs response — tail the most recent log file."""
    import glob
    import os

    if log_dir is None:
        log_dir = "logs"

    # Find the most recent .log file
    patterns = [
        os.path.join(log_dir, "**", "*.log"),
        os.path.join(log_dir, "*.log"),
        "*.log",
    ]

    log_files = []
    for pattern in patterns:
        log_files.extend(glob.glob(pattern, recursive=True))

    if not log_files:
        return _bold("📜 Logs") + "\n\nNo log files found"

    # Sort by modification time, get most recent
    log_files.sort(key=os.path.getmtime, reverse=True)
    latest = log_files[0]

    try:
        with open(latest, "r", encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()

        tail = all_lines[-n:]
        log_name = os.path.basename(latest)
        lines = [
            _bold(f"📜 Logs") + f" ({log_name}, last {len(tail)} lines)",
            "",
            _mono("\n".join(line.rstrip() for line in tail)),
        ]
        return _truncate("\n".join(lines))
    except Exception as e:
        return _bold("📜 Logs") + f"\n\nError reading logs: {e}"


def format_modes(bot: Any) -> str:
    """Format the /modes response showing PB_modes per symbol/side."""
    pb_modes = getattr(bot, "PB_modes", {})

    lines = [_bold("⚙️ Modes"), ""]
    for pside in ("long", "short"):
        side_modes = pb_modes.get(pside, {})
        if not side_modes:
            continue
        side_emoji = "🟢" if pside == "long" else "🔴"
        lines.append(f"{side_emoji} {_bold(pside.upper())}")
        for symbol, mode in sorted(side_modes.items()):
            coin = _coin(symbol)
            lines.append(f"  {coin}: {_mono(mode)}")
        lines.append("")

    if len(lines) <= 2:
        lines.append("No modes configured")

    return _truncate("\n".join(lines))


# ── Internal Helpers ────────────────────────────────────────────────────

def _get_recent_fills(bot: Any, n: int = 10) -> List[Dict[str, Any]]:
    """Get the last N fill events from FillEventsManager."""
    manager = getattr(bot, "_pnls_manager", None)
    if manager is None:
        return []

    try:
        events = manager.get_events()
        if not events:
            return []

        # Convert to dicts and take the last N
        result = []
        for ev in events[-n:]:
            d = ev.to_dict() if hasattr(ev, "to_dict") else {}
            result.append({
                "timestamp": d.get("timestamp", 0),
                "symbol": d.get("symbol", "?"),
                "pside": d.get("position_side", d.get("pside", "?")),
                "side": d.get("side", d.get("trade_side", "?")),
                "type": d.get("pb_order_type", d.get("type", "?")),
                "qty": float(d.get("qty", d.get("amount", 0.0))),
                "price": float(d.get("price", 0.0)),
                "pnl": float(d.get("pnl", 0.0)),
            })
        return result
    except Exception as e:
        logging.warning("[telegram] error reading fills: %s", e)
        return []


def _get_recent_fills_for_symbol(
    bot: Any, symbol: str, n: int = 5
) -> List[Dict[str, Any]]:
    """Get recent fills for a specific symbol."""
    all_fills = _get_recent_fills(bot, n=200)
    symbol_fills = [f for f in all_fills if f.get("symbol") == symbol]
    return symbol_fills[-n:]


def _get_all_fills(bot: Any) -> List[Dict[str, Any]]:
    """Get all fill events from FillEventsManager."""
    manager = getattr(bot, "_pnls_manager", None)
    if manager is None:
        return []

    try:
        events = manager.get_events()
        if not events:
            return []

        result = []
        for ev in events:
            d = ev.to_dict() if hasattr(ev, "to_dict") else {}
            result.append({
                "timestamp": d.get("timestamp", 0),
                "symbol": d.get("symbol", "?"),
                "pside": d.get("position_side", d.get("pside", "?")),
                "side": d.get("side", d.get("trade_side", "?")),
                "type": d.get("pb_order_type", d.get("type", "?")),
                "qty": float(d.get("qty", d.get("amount", 0.0))),
                "price": float(d.get("price", 0.0)),
                "pnl": float(d.get("pnl", 0.0)),
            })
        return result
    except Exception as e:
        logging.warning("[telegram] error reading all fills: %s", e)
        return []


def _classify_fill_event(fill: Dict[str, Any]) -> str:
    """Classify a fill as an entry or close for a position side."""
    pside = str(fill.get("pside", "") or "").lower()
    side = str(fill.get("side", "") or "").lower()
    fill_type = str(fill.get("type", "") or "").lower()

    if "entry" in fill_type:
        return "entry"
    if "close" in fill_type:
        return "close"
    if pside == "long" and side == "buy":
        return "entry"
    if pside == "short" and side == "sell":
        return "entry"
    return "close"


def _extract_completed_position_cycles(fills: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Collapse fills into round-trip position cycles closed to zero."""
    active: Dict[tuple[str, str], Dict[str, Any]] = {}
    completed: List[Dict[str, Any]] = []
    eps = 1e-12

    for fill in sorted(fills, key=lambda row: int(row.get("timestamp", 0) or 0)):
        symbol = str(fill.get("symbol", "?"))
        pside = str(fill.get("pside", "") or "").lower()
        if pside not in ("long", "short"):
            continue
        qty = abs(float(fill.get("qty", 0.0) or 0.0))
        if qty <= eps:
            continue

        key = (symbol, pside)
        event_kind = _classify_fill_event(fill)
        timestamp = int(fill.get("timestamp", 0) or 0)
        price = float(fill.get("price", 0.0) or 0.0)
        pnl = float(fill.get("pnl", 0.0) or 0.0)
        fill_type = str(fill.get("type", "") or "")

        cycle = active.get(key)
        if cycle is None:
            cycle = {
                "symbol": symbol,
                "coin": _coin(symbol),
                "pside": pside,
                "opened_at": timestamp if event_kind == "entry" else None,
                "entry_qty": 0.0,
                "entry_notional": 0.0,
                "close_qty": 0.0,
                "close_notional": 0.0,
                "realized": 0.0,
                "peak_qty": 0.0,
                "open_qty": qty if event_kind == "close" else 0.0,
                "close_type": "",
            }
            if event_kind == "close":
                cycle["peak_qty"] = qty
            active[key] = cycle

        if event_kind == "entry":
            if cycle["opened_at"] is None:
                cycle["opened_at"] = timestamp
            cycle["entry_qty"] += qty
            cycle["entry_notional"] += qty * price
            cycle["open_qty"] += qty
            cycle["peak_qty"] = max(float(cycle["peak_qty"]), float(cycle["open_qty"]))
            continue

        cycle["close_qty"] += qty
        cycle["close_notional"] += qty * price
        cycle["realized"] += pnl
        cycle["close_type"] = fill_type or str(cycle["close_type"])
        cycle["open_qty"] -= qty

        if cycle["open_qty"] < -eps:
            cycle["open_qty"] = 0.0
        if cycle["open_qty"] > eps:
            continue

        completed.append({
            "symbol": symbol,
            "coin": cycle["coin"],
            "pside": pside,
            "opened_at": cycle["opened_at"],
            "closed_at": timestamp,
            "peak_qty": float(cycle["peak_qty"]) if cycle["peak_qty"] else qty,
            "avg_entry": (
                float(cycle["entry_notional"]) / float(cycle["entry_qty"])
                if float(cycle["entry_qty"]) > eps
                else None
            ),
            "avg_close": (
                float(cycle["close_notional"]) / float(cycle["close_qty"])
                if float(cycle["close_qty"]) > eps
                else None
            ),
            "realized": float(cycle["realized"]),
            "close_type": str(cycle["close_type"]),
        })
        active.pop(key, None)

    return completed


def _calc_total_unrealized_pnl(bot: Any) -> float:
    """Calculate total unrealized PnL across all positions."""
    positions = getattr(bot, "positions", {})
    last_prices = getattr(bot, "last_prices", {})
    total = 0.0

    for symbol, pos_data in positions.items():
        c_mult = float(getattr(bot, "c_mults", {}).get(symbol, 1.0))
        current_price = float(last_prices.get(symbol, 0.0))
        for pside in ("long", "short"):
            size = float(pos_data.get(pside, {}).get("size", 0.0))
            price = float(pos_data.get(pside, {}).get("price", 0.0))
            if size != 0.0 and price > 0 and current_price > 0:
                if pside == "long":
                    total += (current_price - price) * abs(size) * c_mult
                else:
                    total += (price - current_price) * abs(size) * c_mult

    return total
