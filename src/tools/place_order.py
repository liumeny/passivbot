#!/usr/bin/env python3
"""
Interactive order placement tool for Binance USDT-M Futures.

Reuses passivbot's api-keys.json for authentication.

Usage:
  ./venv/bin/python3 src/tools/place_order.py --user binance_01

Commands:
  pos     - View current positions
  orders  - View open orders
  place   - Place a limit order
  cancel  - Cancel an open order
  balance - View account balance
  help    - Show help
  quit    - Exit
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import ccxt


# ── API Key Loading (reused from passivbot) ──────────────────────────────────

def load_api_keys(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"api-keys file not found: {path}")
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def get_user_info(api_keys: Dict[str, Any], user: str) -> Dict[str, Any]:
    if user in api_keys and isinstance(api_keys[user], dict):
        return api_keys[user]
    if isinstance(api_keys, dict) and "users" in api_keys and user in api_keys["users"]:
        return api_keys["users"][user]
    raise KeyError(f"user '{user}' not found in api-keys.json")


def build_exchange(user_info: Dict[str, Any]) -> ccxt.Exchange:
    exchange_id = (
        user_info.get("exchange") or user_info.get("exchange_id") or user_info.get("exchangeId")
    )
    if not exchange_id:
        raise KeyError("missing 'exchange' in user info")

    exchange_cls = getattr(ccxt, exchange_id, None) or getattr(ccxt, exchange_id.lower(), None)
    if exchange_cls is None:
        raise Exception(f"exchange '{exchange_id}' not found in ccxt")

    api_key = user_info.get("apiKey") or user_info.get("key") or user_info.get("apikey")
    secret = user_info.get("secret") or user_info.get("apiSecret") or user_info.get("apisecret")
    password = user_info.get("password") or user_info.get("pwd") or user_info.get("passphrase")

    exchange = exchange_cls({
        "apiKey": api_key,
        "secret": secret,
        "password": password,
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })
    return exchange


# ── Display Helpers ──────────────────────────────────────────────────────────

COLORS = {
    "reset": "\033[0m",
    "bold": "\033[1m",
    "red": "\033[91m",
    "green": "\033[92m",
    "yellow": "\033[93m",
    "cyan": "\033[96m",
    "dim": "\033[2m",
}


def c(text: str, color: str) -> str:
    return f"{COLORS.get(color, '')}{text}{COLORS['reset']}"


def print_header(title: str):
    print(f"\n{c('━' * 60, 'dim')}")
    print(f"  {c(title, 'bold')}")
    print(c('━' * 60, 'dim'))


def format_pnl(val: float) -> str:
    if val >= 0:
        return c(f"+{val:.2f}", "green")
    return c(f"{val:.2f}", "red")


def format_side(side: str) -> str:
    if "long" in side.lower() or "buy" in side.lower():
        return c(side.upper(), "green")
    return c(side.upper(), "red")


# ── Core Functions ───────────────────────────────────────────────────────────

def show_balance(exchange: ccxt.Exchange):
    print_header("💰 Account Balance")
    bal = exchange.fetch_balance()
    usdt = bal.get("USDT", {})
    total = float(usdt.get("total", 0))
    free = float(usdt.get("free", 0))
    used = float(usdt.get("used", 0))
    print(f"  Total:     {c(f'{total:.2f}', 'bold')} USDT")
    print(f"  Available: {c(f'{free:.2f}', 'green')} USDT")
    print(f"  In Use:    {c(f'{used:.2f}', 'yellow')} USDT")


def show_positions(exchange: ccxt.Exchange) -> List[dict]:
    print_header("📊 Open Positions")
    positions = exchange.fetch_positions()
    active = [p for p in positions if abs(float(p.get("contracts", 0) or 0)) > 0]
    if not active:
        print(f"  {c('No open positions', 'dim')}")
        return []
    for i, pos in enumerate(active, 1):
        symbol = pos.get("symbol", "?")
        side = pos.get("side", "?")
        size = float(pos.get("contracts", 0) or 0)
        entry = float(pos.get("entryPrice", 0) or 0)
        mark = float(pos.get("markPrice", 0) or 0)
        upnl = float(pos.get("unrealizedPnl", 0) or 0)
        lev = pos.get("leverage", "?")
        liq = pos.get("liquidationPrice")
        liq_str = f"{float(liq):.2f}" if liq else "N/A"
        print(f"  {c(f'[{i}]', 'cyan')} {c(symbol, 'bold')} {format_side(side)} "
              f"x{lev}")
        print(f"      Size: {abs(size):.4f}  Entry: {entry:.2f}  "
              f"Mark: {mark:.2f}  uPnL: {format_pnl(upnl)}")
        print(f"      Liquidation: {liq_str}")
    return active


def show_open_orders(exchange: ccxt.Exchange) -> List[dict]:
    print_header("📋 Open Orders")
    orders = exchange.fetch_open_orders()
    if not orders:
        print(f"  {c('No open orders', 'dim')}")
        return []
    for i, o in enumerate(orders, 1):
        symbol = o.get("symbol", "?")
        side = o.get("side", "?")
        price = float(o.get("price", 0) or 0)
        amount = float(o.get("amount", 0) or 0)
        otype = o.get("type", "?")
        oid = o.get("id", "?")
        info = o.get("info", {})
        pos_side = info.get("positionSide", info.get("posSide", "?"))
        reduce_only = o.get("reduceOnly", False)
        ro_tag = c(" [ReduceOnly]", "yellow") if reduce_only else ""
        print(f"  {c(f'[{i}]', 'cyan')} {c(symbol, 'bold')} | {format_side(side)} "
              f"| {otype} | posSide={pos_side}{ro_tag}")
        print(f"      Price: {price:.4f}  Amount: {amount:.4f}  ID: {c(oid, 'dim')}")
    return orders


def place_order(exchange: ccxt.Exchange):
    print_header("📝 Place Limit Order")

    # Symbol
    symbol = input(f"  Symbol (e.g. ETH/USDT:USDT): ").strip()
    if not symbol:
        print(c("  ✗ Cancelled", "red"))
        return

    # Fetch current price
    try:
        ticker = exchange.fetch_ticker(symbol)
        last_price = float(ticker.get("last", 0))
        print(f"  Current price: {c(f'{last_price:.4f}', 'bold')}")
    except Exception as e:
        print(c(f"  ✗ Failed to fetch price: {e}", "red"))
        return

    # Side
    print(f"\n  Order type:")
    print(f"    {c('1', 'cyan')} = Buy/Long entry    (开多)")
    print(f"    {c('2', 'cyan')} = Sell/Short entry   (开空)")
    print(f"    {c('3', 'cyan')} = Close Long (sell)  (平多)")
    print(f"    {c('4', 'cyan')} = Close Short (buy)  (平空)")
    choice = input(f"  Select [1-4]: ").strip()

    side_map = {
        "1": ("buy",  "LONG",  False),
        "2": ("sell", "SHORT", False),
        "3": ("sell", "LONG",  True),
        "4": ("buy",  "SHORT", True),
    }
    if choice not in side_map:
        print(c("  ✗ Invalid choice", "red"))
        return

    side, pos_side, reduce_only = side_map[choice]

    # Quantity
    qty_str = input(f"  Quantity (contracts): ").strip()
    try:
        qty = float(qty_str)
        if qty <= 0:
            raise ValueError
    except (ValueError, TypeError):
        print(c("  ✗ Invalid quantity", "red"))
        return

    # Price
    price_str = input(f"  Limit price: ").strip()
    try:
        price = float(price_str)
        if price <= 0:
            raise ValueError
    except (ValueError, TypeError):
        print(c("  ✗ Invalid price", "red"))
        return

    # Calculate notional and distance
    notional = qty * price
    dist_pct = ((price - last_price) / last_price) * 100

    # Summary
    print(f"\n  {c('─── Order Summary ───', 'bold')}")
    print(f"  Symbol:       {c(symbol, 'bold')}")
    print(f"  Side:         {format_side(side)}  (Position: {pos_side})")
    print(f"  Reduce Only:  {'Yes' if reduce_only else 'No'}")
    print(f"  Quantity:     {qty:.4f}")
    print(f"  Price:        {price:.4f}  ({dist_pct:+.2f}% from current)")
    print(f"  Notional:     ~{notional:.2f} USDT")
    print(f"  {c('─' * 24, 'bold')}")

    confirm = input(f"\n  {c('Confirm order? [y/N]: ', 'yellow')}").strip().lower()
    if confirm != "y":
        print(c("  ✗ Order cancelled", "red"))
        return

    # Place the order
    try:
        params = {
            "positionSide": pos_side,
        }
        if reduce_only:
            params["reduceOnly"] = True

        result = exchange.create_order(
            symbol=symbol,
            type="limit",
            side=side,
            amount=qty,
            price=price,
            params=params,
        )
        oid = result.get("id", "?")
        print(f"\n  {c('✓ Order placed successfully!', 'green')}")
        print(f"  Order ID: {c(oid, 'cyan')}")
    except Exception as e:
        print(f"\n  {c(f'✗ Failed to place order: {e}', 'red')}")


def cancel_order(exchange: ccxt.Exchange):
    orders = show_open_orders(exchange)
    if not orders:
        return

    print()
    idx_str = input(f"  Select order to cancel [1-{len(orders)}] (0=cancel all, Enter=back): ").strip()
    if not idx_str:
        return

    try:
        idx = int(idx_str)
    except ValueError:
        print(c("  ✗ Invalid input", "red"))
        return

    if idx == 0:
        confirm = input(f"  {c(f'Cancel ALL {len(orders)} orders? [y/N]: ', 'yellow')}").strip().lower()
        if confirm != "y":
            print(c("  ✗ Aborted", "red"))
            return
        for o in orders:
            try:
                exchange.cancel_order(o["id"], o.get("symbol"))
                print(f"  {c('✓', 'green')} Cancelled {o['id']}")
            except Exception as e:
                print(f"  {c('✗', 'red')} Failed: {o['id']} - {e}")
        return

    if idx < 1 or idx > len(orders):
        print(c("  ✗ Invalid index", "red"))
        return

    order = orders[idx - 1]
    confirm = input(
        f"  Cancel {c(order.get('symbol', '?'), 'bold')} "
        f"{format_side(order.get('side', '?'))} @ {order.get('price')}? [y/N]: "
    ).strip().lower()
    if confirm != "y":
        print(c("  ✗ Aborted", "red"))
        return

    try:
        exchange.cancel_order(order["id"], order.get("symbol"))
        print(f"  {c('✓ Order cancelled', 'green')}")
    except Exception as e:
        print(f"  {c(f'✗ Failed to cancel: {e}', 'red')}")


def show_help():
    print_header("📖 Commands")
    cmds = [
        ("pos",     "View current positions          查看持仓"),
        ("orders",  "View open orders                查看挂单"),
        ("place",   "Place a new limit order          下单"),
        ("cancel",  "Cancel an open order             撤单"),
        ("balance", "View account balance             查看余额"),
        ("help",    "Show this help                   帮助"),
        ("quit",    "Exit the program                 退出"),
    ]
    for cmd, desc in cmds:
        print(f"  {c(cmd, 'cyan'):20s} {desc}")


# ── Main Loop ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Interactive order placement tool")
    parser.add_argument("--user", required=True, help="user key in api-keys.json")
    parser.add_argument(
        "--api-keys", default="api-keys.json",
        help="path to api-keys.json (default: api-keys.json)",
    )
    args = parser.parse_args()

    keys_path = Path(args.api_keys)
    api_keys = load_api_keys(keys_path)
    user_info = get_user_info(api_keys, args.user)
    exchange = build_exchange(user_info)

    exchange_name = getattr(exchange, "id", "unknown")
    print(f"\n  {c('═' * 50, 'cyan')}")
    print(f"  {c('  Passivbot Manual Order Tool', 'bold')}")
    print(f"  {c(f'  Exchange: {exchange_name}  |  User: {args.user}', 'dim')}")
    print(f"  {c('═' * 50, 'cyan')}")
    print(f"  Type {c('help', 'cyan')} for available commands\n")

    # Load markets once
    try:
        print(f"  Loading markets...", end="", flush=True)
        exchange.load_markets()
        print(f" {c('done', 'green')}\n")
    except Exception as e:
        print(f"\n  {c(f'Failed to load markets: {e}', 'red')}")
        sys.exit(1)

    while True:
        try:
            cmd = input(f"  {c('>', 'cyan')} ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print(f"\n  {c('Bye!', 'dim')}")
            break

        if not cmd:
            continue
        elif cmd in ("quit", "exit", "q"):
            print(f"  {c('Bye!', 'dim')}")
            break
        elif cmd in ("pos", "positions", "p"):
            try:
                show_positions(exchange)
            except Exception as e:
                print(c(f"  ✗ Error: {e}", "red"))
        elif cmd in ("orders", "o"):
            try:
                show_open_orders(exchange)
            except Exception as e:
                print(c(f"  ✗ Error: {e}", "red"))
        elif cmd in ("place", "new", "n"):
            try:
                place_order(exchange)
            except Exception as e:
                print(c(f"  ✗ Error: {e}", "red"))
        elif cmd in ("cancel", "c"):
            try:
                cancel_order(exchange)
            except Exception as e:
                print(c(f"  ✗ Error: {e}", "red"))
        elif cmd in ("balance", "bal", "b"):
            try:
                show_balance(exchange)
            except Exception as e:
                print(c(f"  ✗ Error: {e}", "red"))
        elif cmd in ("help", "h", "?"):
            show_help()
        else:
            print(f"  Unknown command: {cmd}. Type {c('help', 'cyan')} for options.")


if __name__ == "__main__":
    main()
