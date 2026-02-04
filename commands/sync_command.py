# commands/sync_command.py
"""
Обработчик команды /admin_sync для ручного запуска синхронизации.
"""

from typing import Optional, Dict, Any
from maxapi.types import MessageCreated

from sync_appointments.service import SyncService
from logging_config import log_system_event


class SyncCommandHandler:
    """
    Обработчик админских команд для управления синхронизацией.
    """

    def __init__(self, sync_service: SyncService, admin_id: int):
        """
        Инициализация обработчика команд.

        Args:
            sync_service: Сервис синхронизации
            admin_id: ID администратора
        """
        self.sync_service = sync_service
        self.admin_id = admin_id
        self.is_syncing = False

    async def handle_message(self, event: MessageCreated) -> bool:
        """
        Обрабатывает сообщение, проверяя админские команды.
        """
        try:
            # Используем chat_id для ответов, но user_id для проверки прав
            chat_id = event.message.recipient.chat_id
            user_id = int(event.from_user.user_id) if hasattr(event, 'from_user') and hasattr(event.from_user, 'user_id') else None

            # Проверяем, что это сообщение от администратора (по user_id)
            if user_id != self.admin_id:
                log_system_event("admin_command", "non_admin_attempt", chat_id=str(chat_id), admin_id=str(self.admin_id), user_id=str(user_id))
                return False

            # Проверяем наличие текста сообщения
            if not event.message.body or not event.message.body.text:
                log_system_event("admin_command", "no_text_in_message", chat_id=str(chat_id))
                return False

            message_text = event.message.body.text.strip()

            # Обработка команд
            if message_text == "/admin_sync":
                await self._handle_sync_command(event)
                return True

            elif message_text == "/admin_sync_status":
                await self._handle_status_command(event)
                return True

            elif message_text == "/admin_sync_cleanup":
                await self._handle_cleanup_command(event)
                return True

            elif message_text == "/admin_sync_stats":
                await self._handle_stats_command(event)
                return True

            elif message_text.startswith("/admin_sync_mock"):
                await self._handle_mock_command(event, message_text)
                return True
            
            log_system_event("admin_command", "unknown_sync_command", command=message_text, chat_id=str(chat_id))
            return False

        except Exception as e:
            log_system_event("admin_command", "sync_command_error", error=str(e), chat_id=str(chat_id))
            return False


    async def _handle_sync_command(self, event: MessageCreated) -> None:
        """
        Обрабатывает команду /admin_sync.
        """
        try:
            chat_id = event.message.recipient.chat_id
            log_system_event("admin_sync", "sync_started", chat_id=str(chat_id))
            
            if self.is_syncing:
                log_system_event("admin_sync", "sync_already_running", chat_id=str(chat_id))
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="⏳ Синхронизация уже выполняется. Пожалуйста, подождите."
                )
                return

            self.is_syncing = True

            # Отправляем сообщение о начале
            await event.bot.send_message(
                chat_id=chat_id,
                text="🔄 Запуск ручной синхронизации записей к врачу..."
            )

            # Запускаем синхронизацию
            result = await self.sync_service.run_sync()

            # Формируем отчет
            if result.get('success'):
                summary = result.get('summary', {})
                log_system_event("admin_sync", "sync_completed", 
                               total_received=summary.get('total_received', 0),
                               matched=summary.get('patients_matched', 0),
                               saved=summary.get('new_appointments_saved', 0),
                               duration=result.get('duration_seconds', 0),
                               chat_id=str(chat_id))
                message = (
                    "✅ Синхронизация завершена успешно!\n\n"
                    f"📊 Результаты:\n"
                    f"• Получено записей: {summary.get('total_received', 0)}\n"
                    f"• Успешно обработано: {summary.get('successfully_parsed', 0)}\n"
                    f"• Найдено пациентов: {summary.get('patients_matched', 0)}\n"
                    f"• Сохранено новых записей: {summary.get('new_appointments_saved', 0)}\n"
                    f"• Время выполнения: {result.get('duration_seconds', 0):.2f} сек\n\n"
                    f"⏰ Время завершения: {result.get('timestamp', 'неизвестно')}"
                )
            else:
                error_msg = result.get('error', 'Неизвестная ошибка')
                log_system_event("admin_sync", "sync_failed", error=error_msg, chat_id=str(chat_id))
                message = (
                    "❌ Синхронизация завершена с ошибкой!\n\n"
                    f"Ошибка: {error_msg}\n"
                    f"Время выполнения: {result.get('duration_seconds', 0):.2f} сек"
                )

            await event.bot.send_message(
                chat_id=chat_id,
                text=message
            )

        except Exception as e:
            log_system_event("admin_sync", "sync_command_exception", error=str(e), chat_id=str(chat_id))
            await event.bot.send_message(
                chat_id=event.message.recipient.chat_id,
                text=f"❌ Произошла ошибка при выполнении синхронизации: {str(e)}"
            )
        finally:
            self.is_syncing = False

    async def _handle_status_command(self, event: MessageCreated) -> None:
        """
        Обрабатывает команду /admin_sync_status.
        """
        try:
            chat_id = event.message.recipient.chat_id
            log_system_event("admin_sync", "status_requested", chat_id=str(chat_id))
            status = self.sync_service.get_status()

            last_sync = status.get('last_sync_time', 'никогда')
            last_success = "✅ успешно" if status.get('last_sync_success') else "❌ с ошибкой" if status.get(
                'last_sync_success') is False else "неизвестно"

            db_stats = status.get('database_stats', {})

            message = (
                "📊 Статус системы синхронизации:\n\n"
                f"🕐 Последняя синхронизация: {last_sync}\n"
                f"📈 Результат: {last_success}\n\n"
                f"🗃️ База данных записей:\n"
                f"• Всего записей: {db_stats.get('total_appointments', 0)}\n"
                f"• Уникальных пользователей: {db_stats.get('unique_users', 0)}\n"
                f"• Последнее обновление: {db_stats.get('last_sync', 'неизвестно')}"
            )

            await event.bot.send_message(
                chat_id=event.message.recipient.chat_id,
                text=message
            )

        except Exception as e:
            log_system_event("admin_sync", "status_command_exception", error=str(e), chat_id=str(chat_id))
            await event.bot.send_message(
                chat_id=event.message.recipient.chat_id,
                text=f"❌ Ошибка получения статуса: {str(e)}"
            )

    async def _handle_cleanup_command(self, event: MessageCreated) -> None:
        """
        Обрабатывает команду /admin_sync_cleanup.
        """
        try:
            chat_id = event.message.recipient.chat_id
            log_system_event("admin_sync", "cleanup_started", chat_id=str(chat_id))
            
            await event.bot.send_message(
                chat_id=chat_id,
                text="🗑️ Запуск очистки старых записей (старше 1 года)..."
            )

            result = await self.sync_service.run_cleanup(days_to_keep=365)

            if result.get('success'):
                deleted_count = result.get('deleted_count', 0)
                log_system_event("admin_sync", "cleanup_completed", 
                               deleted_count=deleted_count,
                               duration=result.get('duration_seconds', 0),
                               chat_id=str(chat_id))
                message = (
                    "✅ Очистка завершена успешно!\n\n"
                    f"🗑️ Удалено записей: {deleted_count}\n"
                    f"⏱️ Время выполнения: {result.get('duration_seconds', 0):.2f} сек"
                )
            else:
                error_msg = result.get('error', 'Неизвестная ошибка')
                log_system_event("admin_sync", "cleanup_failed", error=error_msg, chat_id=str(chat_id))
                message = (
                    "❌ Очистка завершена с ошибкой!\n\n"
                    f"Ошибка: {error_msg}"
                )

            await event.bot.send_message(
                chat_id=chat_id,
                text=message
            )

        except Exception as e:
            log_system_event("admin_sync", "cleanup_command_exception", error=str(e), chat_id=str(chat_id))
            await event.bot.send_message(
                chat_id=event.message.recipient.chat_id,
                text=f"❌ Ошибка очистки: {str(e)}"
            )

    async def _handle_stats_command(self, event: MessageCreated) -> None:
        """
        Обрабатывает команду /admin_sync_stats.
        """
        try:
            chat_id = event.message.recipient.chat_id
            log_system_event("admin_sync", "stats_requested", chat_id=str(chat_id))
            status = self.sync_service.get_status()
            components = status.get('components_status', {})

            message = "📈 Детальная статистика компонентов:\n\n"

            # Статистика парсера
            parser_stats = components.get('parser', {})
            if parser_stats:
                message += "📝 Парсер:\n"
                message += f"• Обработано: {parser_stats.get('processed', 0)}\n"
                message += f"• Ошибок: {parser_stats.get('errors', 0)}\n"
                message += f"• Успешность: {parser_stats.get('success_rate', 0):.1f}%\n\n"

            # Статистика матчера
            matcher_stats = components.get('matcher', {})
            if matcher_stats:
                message += "🔍 Матчер:\n"
                message += f"• Найдено: {matcher_stats.get('matched', 0)}\n"
                message += f"• Не найдено: {matcher_stats.get('unmatched', 0)}\n"
                message += f"• Успешность: {matcher_stats.get('match_rate', 0):.1f}%\n\n"

            # Статистика нотификатора
            notifier_stats = components.get('notifier', {})
            if notifier_stats:
                message += "🔔 Нотификатор:\n"
                message += f"• Отправлено: {notifier_stats.get('sent', 0)}\n"
                message += f"• Пропущено: {notifier_stats.get('skipped', 0)}\n"
                message += f"• Ошибок: {notifier_stats.get('errors', 0)}\n"

            await event.bot.send_message(
                chat_id=event.message.recipient.chat_id,
                text=message
            )

        except Exception as e:
            log_system_event("admin_sync", "stats_command_exception", error=str(e), chat_id=str(chat_id))
            await event.bot.send_message(
                chat_id=event.message.recipient.chat_id,
                text=f"❌ Ошибка получения статистики: {str(e)}"
            )

    async def _handle_mock_command(self, event: MessageCreated, message_text: str) -> None:
        """
        Обрабатывает команду /admin_sync_mock [путь_к_файлу].
        """
        try:
            chat_id = event.message.recipient.chat_id
            # Извлекаем путь к файлу из команды
            parts = message_text.split()
            if len(parts) < 2:
                log_system_event("admin_sync", "mock_invalid_command", command=message_text, chat_id=str(chat_id))
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="❌ Укажите путь к мок-файлу: /admin_sync_mock [путь_к_файлу]"
                )
                return

            mock_file_path = parts[1]
            log_system_event("admin_sync", "mock_started", file_path=mock_file_path, chat_id=str(chat_id))

            if self.is_syncing:
                log_system_event("admin_sync", "mock_already_running", chat_id=str(chat_id))
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="⏳ Синхронизация уже выполняется. Пожалуйста, подождите."
                )
                return

            self.is_syncing = True

            await event.bot.send_message(
                chat_id=chat_id,
                text=f"🧪 Запуск тестовой синхронизации с мок-данными из {mock_file_path}..."
            )

            # Запускаем тестовую синхронизацию
            result = await self.sync_service.force_sync_with_mock(mock_file_path)

            # Формируем отчет
            if result.get('success'):
                summary = result.get('summary', {})
                message = (
                    "🧪 Тестовая синхронизация завершена!\n\n"
                    f"📊 Результаты:\n"
                    f"• Получено записей: {summary.get('total_received', 0)}\n"
                    f"• Успешно обработано: {summary.get('successfully_parsed', 0)}\n"
                    f"• Найдено пациентов: {summary.get('patients_matched', 0)}\n"
                    f"• Сохранено новых записей: {summary.get('new_appointments_saved', 0)}\n"
                    f"• Время выполнения: {result.get('duration_seconds', 0):.2f} сек"
                )
            else:
                message = (
                    "❌ Тестовая синхронизация завершена с ошибкой!\n\n"
                    f"Ошибка: {result.get('error', 'Неизвестная ошибка')}"
                )

            await event.bot.send_message(
                chat_id=event.message.recipient.chat_id,
                text=message
            )

        except Exception as e:
            log_system_event("admin_sync", "mock_command_exception", error=str(e), chat_id=str(chat_id))
            await event.bot.send_message(
                chat_id=event.message.recipient.chat_id,
                text=f"❌ Ошибка тестовой синхронизации: {str(e)}"
            )
        finally:
            self.is_syncing = False

    async def handle_callback(self, event, payload: str) -> bool:
        """
        Обрабатывает callback-и от кнопок уведомлений.

        Args:
            event: Событие callback
            payload: Данные callback

        Returns:
            True если callback обработан, False если нет
        """
        try:
            chat_id = event.message.recipient.chat_id
            user_id = int(event.from_user.user_id) if hasattr(event, 'from_user') and hasattr(event.from_user, 'user_id') else None

            # Пропускаем, если не от администратора
            if user_id != self.admin_id:
                return False

            # Обработка callback-ов администратора для управления синхронизацией
            if payload.startswith("sync_"):
                action = payload.split(":")[0] if ":" in payload else payload
                log_system_event("admin_callback", "sync_action", action=action, chat_id=str(chat_id))

                if action == "sync_start":
                    await self._handle_sync_command(event)
                    return True
                elif action == "sync_status":
                    await self._handle_status_command(event)
                    return True
                elif action == "sync_cleanup":
                    await self._handle_cleanup_command(event)
                    return True

            return False

        except Exception as e:
            log_system_event("admin_callback", "sync_callback_error", error=str(e), chat_id=str(chat_id))
            return False