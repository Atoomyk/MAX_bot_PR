import asyncio
import time
from typing import Optional, Tuple

import aiohttp

from bot_utils import create_keyboard
from logging_config import log_system_event


class MisHealthGuard:
    """Следит за доступностью МИС и предоставляет guard для пользовательских сценариев."""

    USER_UNAVAILABLE_TEXT = (
        "Сервис временно недоступен. Проходит техническое обслуживание. "
        "Как только работы завершатся, чат-бот снова станет доступен."
    )
    ADMIN_DOWN_TEXT = "Сервис МИС не доступен!"
    ADMIN_UP_TEXT = "Сервис МИС снова доступен"

    def __init__(
        self,
        bot,
        admin_id: Optional[int],
        healthcheck_url: Optional[str],
        check_interval_sec: int = 30,
        fast_check_interval_sec: int = 2,
        timeout_sec: int = 2,
        fail_threshold: int = 3,
        success_threshold: int = 3,
        admin_down_cooldown_sec: int = 300,
    ) -> None:
        self.bot = bot
        self.admin_id = admin_id
        self.healthcheck_url = (healthcheck_url or "").strip()
        self.check_interval_sec = max(1, int(check_interval_sec))
        self.fast_check_interval_sec = max(1, int(fast_check_interval_sec))
        self.timeout_sec = max(1, int(timeout_sec))
        self.fail_threshold = max(1, int(fail_threshold))
        self.success_threshold = max(1, int(success_threshold))
        self.admin_down_cooldown_sec = max(1, int(admin_down_cooldown_sec))

        # Fail-closed: до подтвержденного восстановления считаем МИС недоступной.
        # Важно: user-block и "подтвержденное состояние" разделены:
        # - _is_available: доступность для пользовательских МИС-сценариев
        # - _confirmed_state: подтвержденный статус канала для логов/уведомлений админа
        self._is_available = False
        self._confirmed_state = "UNKNOWN"  # UNKNOWN | UP | DOWN
        self._consecutive_fail = 0
        self._consecutive_success = 0
        self._last_error = "initial_state"
        self._last_down_notification_ts = 0.0
        self._running = True
        self._fast_mode = False

    async def bootstrap(self) -> None:
        """Быстрая инициализация состояния до старта вебхука."""
        if not self.healthcheck_url:
            self._last_error = "MIS_HEALTHCHECK_URL is missing"
            # Fail-closed: блокируем пользователей сразу, подтверждаем DOWN и уведомляем админа.
            self._is_available = False
            self._consecutive_fail = self.fail_threshold
            await self._confirm_down(self._last_error)
            return

        # Быстрый прогрев для подтверждения начального состояния.
        attempts = max(self.fail_threshold, self.success_threshold)
        self._fast_mode = True
        for idx in range(attempts):
            success, details = await self._probe_once()
            await self._process_probe_result(success, details)
            # Останавливаем bootstrap после первого подтвержденного состояния.
            if self._confirmed_state in ("UP", "DOWN"):
                break
            if idx < attempts - 1:
                await asyncio.sleep(self.fast_check_interval_sec)
        self._fast_mode = False

    async def run(self) -> None:
        """Фоновый воркер health-check."""
        while self._running:
            try:
                success, details = await self._probe_once()
                await self._process_probe_result(success, details)
                sleep_for = self.fast_check_interval_sec if self._fast_mode else self.check_interval_sec
                await asyncio.sleep(sleep_for)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._last_error = f"health_worker_exception: {e}"
                await self._process_probe_result(False, self._last_error)
                sleep_for = self.fast_check_interval_sec if self._fast_mode else self.check_interval_sec
                await asyncio.sleep(sleep_for)

    def stop(self) -> None:
        self._running = False

    def is_mis_available(self) -> bool:
        return self._is_available

    async def notify_user_mis_unavailable(self, chat_id: int, user_id: int, source: str) -> None:
        """Отправляет пользователю единое сообщение о недоступности МИС."""
        keyboard = create_keyboard(
            [[{"type": "callback", "text": "🏠 Главное меню", "payload": "back_to_main"}]]
        )
        await self.bot.send_message(
            chat_id=chat_id,
            text=self.USER_UNAVAILABLE_TEXT,
            attachments=[keyboard] if keyboard else [],
        )
        log_system_event(
            "mis_health",
            "user_notified_unavailable",
            user_id=user_id,
            chat_id=chat_id,
            source=source,
        )

    async def _probe_once(self) -> Tuple[bool, str]:
        if not self.healthcheck_url:
            return False, "MIS_HEALTHCHECK_URL is missing"

        timeout = aiohttp.ClientTimeout(total=self.timeout_sec)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self.healthcheck_url) as response:
                    status = response.status
                    if status >= 400:
                        return False, f"http_status={status}"
                    body = await response.text()
                    if not body or not body.strip():
                        return False, "empty_response"
                    return True, f"http_status={status}"
        except asyncio.TimeoutError:
            return False, "timeout"
        except aiohttp.ClientError as e:
            return False, f"client_error={type(e).__name__}"
        except Exception as e:
            return False, f"unexpected_error={type(e).__name__}:{e}"

    async def _process_probe_result(self, success: bool, details: str) -> None:
        if success:
            self._consecutive_success += 1
            self._consecutive_fail = 0
            # Если МИС ранее была подтвержденно DOWN и появился первый успех —
            # запускаем быстрые проверки до подтверждения восстановления.
            if self._confirmed_state == "DOWN":
                self._fast_mode = True

            # Разблокировка пользователя и подтверждение UP — только после 3 подряд успехов.
            if self._consecutive_success >= self.success_threshold:
                self._is_available = True
                self._last_error = ""
                await self._confirm_up()
                self._fast_mode = False
            return

        self._consecutive_fail += 1
        self._consecutive_success = 0
        self._last_error = details
        # Блокируем пользователей сразу после первого fail.
        self._is_available = False
        # После первого fail ускоряем проверки для подтверждения DOWN.
        self._fast_mode = True
        if self._consecutive_fail >= self.fail_threshold:
            await self._confirm_down(details)
            self._fast_mode = False

    async def _confirm_down(self, details: str) -> None:
        """Подтверждает состояние DOWN (3 подряд fail), логирует и уведомляет админа."""
        if self._confirmed_state != "DOWN":
            self._confirmed_state = "DOWN"
            log_system_event(
                "mis_health",
                "mis_unavailable",
                message=f"МИС НЕ ДОСТУПНА - ошибка: {details}",
            )
        await self._send_admin_down_notification()

    async def _confirm_up(self) -> None:
        """Подтверждает состояние UP (3 подряд success), логирует и при необходимости уведомляет админа."""
        previous_state = self._confirmed_state
        if self._confirmed_state != "UP":
            self._confirmed_state = "UP"
            log_system_event("mis_health", "mis_available", message="МИС ДОСТУПНА - 200 OK")
        # Сообщение "МИС снова доступен" отправляем только после восстановления из DOWN.
        if previous_state == "DOWN":
            await self._send_admin_up_notification()

    async def _send_admin_down_notification(self) -> None:
        admin_chat_id = self._get_admin_chat_id()
        if not admin_chat_id:
            return
        now = time.time()
        if now - self._last_down_notification_ts < self.admin_down_cooldown_sec:
            return
        try:
            await self.bot.send_message(chat_id=admin_chat_id, text=self.ADMIN_DOWN_TEXT)
            self._last_down_notification_ts = now
        except Exception as e:
            log_system_event("mis_health", "admin_down_notify_failed", error=str(e))

    async def _send_admin_up_notification(self) -> None:
        admin_chat_id = self._get_admin_chat_id()
        if not admin_chat_id:
            return
        try:
            await self.bot.send_message(chat_id=admin_chat_id, text=self.ADMIN_UP_TEXT)
        except Exception as e:
            log_system_event("mis_health", "admin_up_notify_failed", error=str(e))

    def _get_admin_chat_id(self) -> Optional[int]:
        """Возвращает chat_id администратора по его user_id."""
        if not self.admin_id:
            return None
        try:
            # Локальный импорт, чтобы не создавать лишних циклических зависимостей.
            from user_database import db

            admin_chat_id = db.get_last_chat_id(self.admin_id)
            if not admin_chat_id:
                log_system_event(
                    "mis_health",
                    "admin_chat_id_not_found",
                    admin_id=self.admin_id,
                )
                return None
            return int(admin_chat_id)
        except Exception as e:
            log_system_event("mis_health", "admin_chat_id_lookup_failed", error=str(e), admin_id=self.admin_id)
            return None
