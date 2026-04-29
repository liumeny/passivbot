"""TelegramService — lifecycle management for the Passivbot Telegram bot.

This module owns the Telegram polling lifecycle, authentication middleware,
and message dispatch. It never touches trading logic directly.
"""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

try:
    from telegram import Bot, BotCommand, ReplyKeyboardMarkup, Update
    from telegram.ext import (
        Application,
        CommandHandler,
        ContextTypes,
        filters,
    )
    from telegram.constants import ParseMode

    HAS_TELEGRAM = True
except ImportError:
    HAS_TELEGRAM = False


class TelegramService:
    """Manages Telegram bot lifecycle: polling, auth, and message sending."""

    def __init__(self, config: dict, bot_instance: Any):
        """
        Args:
            config: The ``telegram`` section of the passivbot config.
            bot_instance: The live ``Passivbot`` object (stored for read-model access).
        """
        if not HAS_TELEGRAM:
            raise ImportError(
                "python-telegram-bot is not installed. "
                "Install it with: pip install 'python-telegram-bot>=21.0'"
            )

        self.token: str = str(config.get("token", "") or "")
        self.chat_id: str = str(config.get("chat_id", "") or "")
        self.credentials_path: str = str(
            config.get("credentials_path")
            or config.get("telegram_credentials_path")
            or ""
        )
        if self.credentials_path and (not self.token or not self.chat_id):
            token, chat_id = self._load_credentials_from_path(self.credentials_path)
            if not self.token:
                self.token = token
            if not self.chat_id:
                self.chat_id = chat_id
        self.topic_id: Optional[int] = (
            int(config["topic_id"]) if config.get("topic_id") else None
        )
        self.authorized_users: List[str] = [
            str(u) for u in config.get("authorized_users", [])
        ]
        self.read_only: bool = bool(config.get("read_only", False))
        self.keyboard_layout: List[List[str]] = config.get("keyboard", [])
        if not self.keyboard_layout:
            self.keyboard_layout = [
                ["账务总余额", "当前持仓"],
                ["过去24h仓位", "帮助"],
            ]

        self._bot_instance = bot_instance
        self._app: Optional[Application] = None
        self._polling_task: Optional[asyncio.Task] = None
        self._started = False
        self._reply_markup: Optional[ReplyKeyboardMarkup] = None

        # Keep access tokens out of logs; httpx/httpcore default INFO logs include full URLs.
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)

        if not self.token:
            raise ValueError("Telegram token is missing")
        if not self.chat_id:
            raise ValueError("Telegram chat_id is missing")

    @staticmethod
    def _load_credentials_from_path(path: str) -> tuple[str, str]:
        """Load bot token and chat_id from the existing credentials text file."""
        text = Path(path).read_text(encoding="utf-8")
        token_match = re.search(r"(?m)^\s*(\d{6,}:[A-Za-z0-9_-]{20,})\s*$", text)
        chat_id_match = re.search(r"(?im)^\s*chatid\s*:\s*(.+?)\s*$", text)
        if token_match is None:
            raise ValueError(f"Telegram credentials file {path} is missing a token line")
        if chat_id_match is None:
            raise ValueError(f"Telegram credentials file {path} is missing a chatId line")
        return token_match.group(1).strip(), chat_id_match.group(1).strip()

    def get_reply_markup(self) -> Optional[ReplyKeyboardMarkup]:
        """Return the configured persistent keyboard, if any."""
        return self._reply_markup

    # ── Authentication ──────────────────────────────────────────────────

    def _is_authorized(self, update: Update) -> bool:
        """Check if the incoming update is from an authorized chat/user."""
        if not update.effective_chat:
            return False

        # Chat ID check (required)
        if str(update.effective_chat.id) != self.chat_id:
            return False

        # Topic ID check (optional, for group chats with topics)
        if self.topic_id is not None:
            msg = update.effective_message
            if msg and getattr(msg, "message_thread_id", None) != self.topic_id:
                return False

        # User whitelist check (optional)
        if self.authorized_users and update.effective_user:
            if str(update.effective_user.id) not in self.authorized_users:
                return False

        return True

    # ── Message Sending ─────────────────────────────────────────────────

    async def send_message(
        self,
        text: str,
        parse_mode: Optional[str] = ParseMode.HTML,
        disable_notification: bool = False,
        include_keyboard: bool = True,
    ) -> None:
        """Send a message to the configured chat, handling errors gracefully."""
        if not self._app or not self._app.bot:
            return

        try:
            kwargs: Dict[str, Any] = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_notification": disable_notification,
            }
            if self.topic_id is not None:
                kwargs["message_thread_id"] = self.topic_id
            if include_keyboard and self._reply_markup is not None:
                kwargs["reply_markup"] = self._reply_markup

            await self._app.bot.send_message(**kwargs)
        except Exception as e:
            logging.warning("[telegram] failed to send message: %s", e)

    # ── Lifecycle ───────────────────────────────────────────────────────

    async def start(self, command_registrar: Callable) -> None:
        """Build the Application, register commands, and start polling.

        Args:
            command_registrar: A callable that receives (Application, TelegramService)
                               and registers all command handlers.
        """
        if self._started:
            return

        self._app = (
            Application.builder()
            .token(self.token)
            .build()
        )
        self._reply_markup = ReplyKeyboardMarkup(
            self.keyboard_layout,
            resize_keyboard=True,
            is_persistent=True,
            input_field_placeholder="选择查询项",
        )

        # Store references for use in handlers
        self._app.bot_data["bot"] = self._bot_instance
        self._app.bot_data["telegram_service"] = self

        # Let the command module register handlers
        command_registrar(self._app, self)

        # Initialize the application
        await self._app.initialize()
        await self._app.start()
        try:
            await self._app.bot.set_my_commands(
                [
                    BotCommand("help", "查看帮助"),
                    BotCommand("balance", "查看账务总余额"),
                    BotCommand("status", "查看当前持仓"),
                    BotCommand("positions24h", "查看过去24h仓位"),
                ]
            )
        except Exception as e:
            logging.warning("[telegram] failed to set commands: %s", e)

        # Start polling in a background task (does not block)
        self._polling_task = asyncio.create_task(self._poll_loop())
        self._started = True
        logging.info("[telegram] service started (chat_id=%s)", self.chat_id)

    async def _poll_loop(self) -> None:
        """Run the updater polling loop in background."""
        try:
            updater = self._app.updater
            await updater.start_polling(drop_pending_updates=True)
            # Keep alive until cancelled
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logging.info("[telegram] polling cancelled")
        except Exception as e:
            logging.error("[telegram] polling error: %s", e)

    async def stop(self) -> None:
        """Gracefully shut down Telegram polling and application."""
        if not self._started:
            return

        try:
            if self._polling_task:
                self._polling_task.cancel()
                try:
                    await self._polling_task
                except asyncio.CancelledError:
                    pass

            if self._app:
                if self._app.updater and self._app.updater.running:
                    await self._app.updater.stop()
                await self._app.stop()
                await self._app.shutdown()
        except Exception as e:
            logging.warning("[telegram] error during shutdown: %s", e)
        finally:
            self._started = False
            logging.info("[telegram] service stopped")

    @property
    def is_running(self) -> bool:
        return self._started
