"""TelegramCommands — command handlers for the Passivbot Telegram bot.

Each handler validates auth, extracts arguments, calls the read model,
and sends the response. Control commands enqueue actions for the live loop.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

try:
    from telegram import Update
    from telegram.ext import (
        Application,
        CommandHandler,
        ContextTypes,
        MessageHandler,
        filters,
    )
    from telegram.constants import ParseMode

    HAS_TELEGRAM = True
except ImportError:
    HAS_TELEGRAM = False

from telegram_read_model import (
    format_help,
    format_health,
    format_status,
    format_status_detail,
    format_balance,
    format_fills,
    format_profit,
    format_daily,
    format_positions_24h,
    format_version,
    format_logs,
    format_modes,
)


# ── Control Actions ─────────────────────────────────────────────────────

@dataclass
class ControlAction:
    """Base class for actions enqueued from Telegram to the live loop."""
    action_type: str = ""
    user_id: str = ""
    details: dict = field(default_factory=dict)


@dataclass
class SetModeAction(ControlAction):
    """Set PB_mode_stop for a side."""
    action_type: str = "set_mode"
    side: str = ""  # "long" or "short"
    mode: str = "graceful_stop"


@dataclass
class RestartAction(ControlAction):
    """Request bot restart."""
    action_type: str = "restart"


# ── Auth Wrapper ────────────────────────────────────────────────────────

def _get_service(context: ContextTypes.DEFAULT_TYPE):
    """Get the TelegramService from context bot_data."""
    return context.bot_data.get("telegram_service")


def _get_bot(context: ContextTypes.DEFAULT_TYPE):
    """Get the Passivbot instance from context bot_data."""
    return context.bot_data.get("bot")


def _get_control_queue(context: ContextTypes.DEFAULT_TYPE) -> Optional[asyncio.Queue]:
    """Get the control action queue from the bot instance."""
    bot = _get_bot(context)
    return getattr(bot, "_control_queue", None) if bot else None


async def _reply(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    *,
    parse_mode: Optional[str] = ParseMode.HTML,
) -> None:
    """Reply with the configured keyboard when available."""
    if not update.message:
        return
    service = _get_service(context)
    kwargs: dict[str, Any] = {"parse_mode": parse_mode}
    if service and getattr(service, "get_reply_markup", None):
        reply_markup = service.get_reply_markup()
        if reply_markup is not None:
            kwargs["reply_markup"] = reply_markup
    await update.message.reply_text(text, **kwargs)


async def _check_auth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check authorization. Returns False and replies if unauthorized."""
    service = _get_service(context)
    if not service:
        return False
    if not service._is_authorized(update):
        logging.warning(
            "[telegram] unauthorized access attempt from chat=%s user=%s",
            update.effective_chat.id if update.effective_chat else "?",
            update.effective_user.id if update.effective_user else "?",
        )
        return False
    return True


async def _check_write_auth(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    """Check auth + write permission. Returns False for read-only mode."""
    if not await _check_auth(update, context):
        return False
    service = _get_service(context)
    if service and service.read_only:
        await _reply(update, context, "⛔ Bot is in read-only mode. Control commands are disabled.")
        return False
    return True


# ── Read Command Handlers ──────────────────────────────────────────────

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _check_auth(update, context):
        return
    service = _get_service(context)
    read_only = service.read_only if service else True
    await _reply(update, context, format_help(read_only=read_only))


async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _check_auth(update, context):
        return
    bot = _get_bot(context)
    if not bot:
        await _reply(update, context, "❌ Bot not available")
        return
    await _reply(update, context, format_health(bot))


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _check_auth(update, context):
        return
    bot = _get_bot(context)
    if not bot:
        await _reply(update, context, "❌ Bot not available")
        return

    # Check if a specific coin was requested
    if context.args:
        coin = context.args[0].upper()
        await _reply(update, context, format_status_detail(bot, coin))
    else:
        await _reply(update, context, format_status(bot))


async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _check_auth(update, context):
        return
    bot = _get_bot(context)
    if not bot:
        await _reply(update, context, "❌ Bot not available")
        return
    await _reply(update, context, await format_balance(bot))


async def cmd_fills(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _check_auth(update, context):
        return
    bot = _get_bot(context)
    if not bot:
        await _reply(update, context, "❌ Bot not available")
        return

    n = 10
    if context.args:
        try:
            n = min(int(context.args[0]), 50)
        except ValueError:
            pass
    await _reply(update, context, format_fills(bot, n))


async def cmd_profit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _check_auth(update, context):
        return
    bot = _get_bot(context)
    if not bot:
        await _reply(update, context, "❌ Bot not available")
        return

    window = context.args[0] if context.args else None
    await _reply(update, context, format_profit(bot, window))


async def cmd_daily(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _check_auth(update, context):
        return
    bot = _get_bot(context)
    if not bot:
        await _reply(update, context, "❌ Bot not available")
        return

    n = 7
    if context.args:
        try:
            n = min(int(context.args[0]), 30)
        except ValueError:
            pass
    await _reply(update, context, format_daily(bot, n))


async def cmd_positions_24h(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _check_auth(update, context):
        return
    bot = _get_bot(context)
    if not bot:
        await _reply(update, context, "❌ Bot not available")
        return
    await _reply(update, context, format_positions_24h(bot))


async def cmd_version(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _check_auth(update, context):
        return
    await _reply(update, context, format_version())


async def cmd_logs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _check_auth(update, context):
        return

    n = 20
    if context.args:
        try:
            n = min(int(context.args[0]), 50)
        except ValueError:
            pass

    # Try to find log directory from bot config
    bot = _get_bot(context)
    log_dir = None
    if bot:
        config = getattr(bot, "config", {})
        log_dir = config.get("logging", {}).get("log_dirpath", None)

    await _reply(update, context, format_logs(log_dir, n))


async def cmd_modes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _check_auth(update, context):
        return
    bot = _get_bot(context)
    if not bot:
        await _reply(update, context, "❌ Bot not available")
        return
    await _reply(update, context, format_modes(bot))


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Open the Telegram bot with help text and the persistent keyboard."""
    await cmd_help(update, context)


async def cmd_menu_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Route reply-keyboard button text to the corresponding handler."""
    if not await _check_auth(update, context):
        return
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip()
    mapping = {
        "账务总余额": cmd_balance,
        "总余额": cmd_balance,
        "当前持仓": cmd_status,
        "过去24h仓位": cmd_positions_24h,
        "24h仓位": cmd_positions_24h,
        "帮助": cmd_help,
    }
    handler = mapping.get(text)
    if handler is not None:
        await handler(update, context)


# ── Control Command Handlers ───────────────────────────────────────────

async def cmd_graceful_stop(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not await _check_write_auth(update, context):
        return

    if not context.args or context.args[0].lower() not in ("long", "short"):
        await _reply(update, context, "Usage: /graceful_stop &lt;long|short&gt;")
        return

    side = context.args[0].lower()
    queue = _get_control_queue(context)
    if queue is None:
        await _reply(update, context, "❌ Control queue not available")
        return

    user_id = str(update.effective_user.id) if update.effective_user else "?"
    action = SetModeAction(
        side=side,
        mode="graceful_stop",
        user_id=user_id,
    )
    await queue.put(action)

    await _reply(
        update,
        context,
        f"✅ Queued: <b>graceful_stop</b> for <code>{side}</code>\n"
        f"Will take effect at the next execution checkpoint.",
    )
    logging.info("[telegram] user=%s queued graceful_stop for %s", user_id, side)


async def cmd_restart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _check_write_auth(update, context):
        return

    queue = _get_control_queue(context)
    if queue is None:
        await _reply(update, context, "❌ Control queue not available")
        return

    user_id = str(update.effective_user.id) if update.effective_user else "?"
    action = RestartAction(user_id=user_id)
    await queue.put(action)

    await _reply(
        update,
        context,
        "✅ Queued: <b>restart</b>\n"
        "Bot will restart at the next execution checkpoint.",
    )
    logging.info("[telegram] user=%s queued restart", user_id)


# ── Registration ────────────────────────────────────────────────────────

def register_commands(app: Application, service: Any) -> None:
    """Register all command handlers with the Application.

    This is passed as the ``command_registrar`` to ``TelegramService.start()``.
    """
    # Read commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_help))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("health", cmd_health))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("balance", cmd_balance))
    app.add_handler(CommandHandler("positions24h", cmd_positions_24h))
    app.add_handler(CommandHandler("pos24h", cmd_positions_24h))
    app.add_handler(CommandHandler("fills", cmd_fills))
    app.add_handler(CommandHandler("profit", cmd_profit))
    app.add_handler(CommandHandler("daily", cmd_daily))
    app.add_handler(CommandHandler("version", cmd_version))
    app.add_handler(CommandHandler("logs", cmd_logs))
    app.add_handler(CommandHandler("modes", cmd_modes))

    # Control commands
    app.add_handler(CommandHandler("graceful_stop", cmd_graceful_stop))
    app.add_handler(CommandHandler("restart", cmd_restart))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_menu_text))

    logging.info("[telegram] registered %d command handlers", 17)
