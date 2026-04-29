from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Sequence


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WalletBalanceDistributionEntry:
    wallet_name: str
    amount_quote: float
    pct_of_total: float


@dataclass(frozen=True)
class AccountBalanceDistributionSnapshot:
    attempted: bool = False
    total_balance: float | None = None
    wallets: tuple[WalletBalanceDistributionEntry, ...] = ()
    partial: bool = False
    unavailable: bool = False
    reason: str | None = None


async def _fetch_btc_quote_price(bot: Any, quote: str) -> float | None:
    if quote == "BTC":
        return 1.0
    client = getattr(bot, "cca", None)
    fetch_ticker = getattr(client, "fetch_ticker", None)
    if not callable(fetch_ticker):
        logger.warning(
            "[telegram] BTC/%s price unavailable for %s:%s: fetch_ticker missing",
            quote,
            getattr(bot, "exchange", "?"),
            getattr(bot, "user", "?"),
        )
        return None
    symbol = f"BTC/{quote}"
    try:
        ticker = await fetch_ticker(symbol)
    except Exception as exc:
        logger.warning(
            "[telegram] BTC/%s price unavailable for %s:%s: %s",
            quote,
            getattr(bot, "exchange", "?"),
            getattr(bot, "user", "?"),
            exc,
        )
        return None
    try:
        last_price = float(ticker["last"])
    except (KeyError, TypeError, ValueError):
        logger.warning(
            "[telegram] BTC/%s price unavailable for %s:%s: invalid ticker payload %r",
            quote,
            getattr(bot, "exchange", "?"),
            getattr(bot, "user", "?"),
            ticker,
        )
        return None
    if not math.isfinite(last_price) or last_price <= 0.0:
        logger.warning(
            "[telegram] BTC/%s price unavailable for %s:%s: non-positive last price %r",
            quote,
            getattr(bot, "exchange", "?"),
            getattr(bot, "user", "?"),
            last_price,
        )
        return None
    return last_price


async def build_account_balance_distribution_snapshot(bot: Any) -> AccountBalanceDistributionSnapshot:
    exchange = str(getattr(bot, "exchange", "") or "").lower()
    if exchange != "binance":
        return AccountBalanceDistributionSnapshot()

    client = getattr(bot, "cca", None)
    fetch_wallet_balance = getattr(client, "sapi_get_asset_wallet_balance", None)
    if not callable(fetch_wallet_balance):
        return AccountBalanceDistributionSnapshot()

    try:
        fetched = await fetch_wallet_balance()
    except Exception as exc:
        logger.warning(
            "[telegram] account total balance unavailable for %s:%s: %s",
            getattr(bot, "exchange", "?"),
            getattr(bot, "user", "?"),
            exc,
        )
        return AccountBalanceDistributionSnapshot(
            attempted=True,
            unavailable=True,
            reason="wallet_balance_unavailable",
        )

    if not isinstance(fetched, Sequence) or isinstance(fetched, (str, bytes, bytearray)):
        logger.warning(
            "[telegram] unexpected wallet balance payload for %s:%s: %r",
            getattr(bot, "exchange", "?"),
            getattr(bot, "user", "?"),
            type(fetched).__name__,
        )
        return AccountBalanceDistributionSnapshot(
            attempted=True,
            partial=True,
            unavailable=True,
            reason="wallet_balance_payload_invalid",
        )

    positive_wallets_btc: list[tuple[str, float]] = []
    partial = False
    for entry in fetched:
        if not isinstance(entry, dict):
            partial = True
            continue
        if entry.get("activate") is not True:
            continue
        wallet_name = str(entry.get("walletName", "") or "").strip()
        if not wallet_name:
            partial = True
            continue
        try:
            balance_btc = float(entry["balance"])
        except (KeyError, TypeError, ValueError):
            partial = True
            continue
        if not math.isfinite(balance_btc):
            partial = True
            continue
        if balance_btc <= 0.0:
            continue
        positive_wallets_btc.append((wallet_name, balance_btc))

    if not positive_wallets_btc:
        return AccountBalanceDistributionSnapshot(
            attempted=True,
            total_balance=0.0,
            partial=partial,
        )

    quote = str(getattr(bot, "quote", "USDT") or "USDT").upper()
    btc_quote_price = await _fetch_btc_quote_price(bot, quote)
    if btc_quote_price is None:
        return AccountBalanceDistributionSnapshot(
            attempted=True,
            partial=partial,
            unavailable=True,
            reason="btc_quote_price_unavailable",
        )

    wallets = [
        WalletBalanceDistributionEntry(
            wallet_name=wallet_name,
            amount_quote=balance_btc * btc_quote_price,
            pct_of_total=0.0,
        )
        for wallet_name, balance_btc in positive_wallets_btc
    ]
    wallets.sort(key=lambda entry: (-entry.amount_quote, entry.wallet_name))
    total_balance = sum(entry.amount_quote for entry in wallets)
    if total_balance <= 0.0:
        return AccountBalanceDistributionSnapshot(
            attempted=True,
            total_balance=0.0,
            partial=partial,
        )

    normalized_wallets = tuple(
        WalletBalanceDistributionEntry(
            wallet_name=entry.wallet_name,
            amount_quote=entry.amount_quote,
            pct_of_total=entry.amount_quote / total_balance * 100.0,
        )
        for entry in wallets
    )
    return AccountBalanceDistributionSnapshot(
        attempted=True,
        total_balance=total_balance,
        wallets=normalized_wallets,
        partial=partial,
    )
