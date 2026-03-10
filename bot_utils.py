# bot_utils.py
"""Утилиты и вспомогательные функции бота"""
import asyncio
import time
import aiohttp
from functools import wraps
from maxapi import Bot
from maxapi.types import Attachment, ButtonsPayload, CallbackButton, LinkButton, RequestContactButton
from maxapi.utils.inline_keyboard import AttachmentType

from logging_config import log_system_event, set_logging_user_id, clear_logging_user_id

# Импортируем константы URL из bot_config (ленивый импорт, чтобы избежать циклического импорта)
def _get_url_constants():
    """Получает константы URL из bot_config (ленивый импорт)"""
    from bot_config import (
        GOSUSLUGI_APPOINTMENT_URL,
        GOSUSLUGI_MEDICAL_EXAM_URL,
        GOSUSLUGI_DOCTOR_HOME_URL,
        GOSUSLUGI_ATTACH_TO_POLYCLINIC_URL,
        CONTACT_CENTER_URL,
        MAP_OF_MEDICAL_INSTITUTIONS_URL
    )
    return {
        'GOSUSLUGI_APPOINTMENT_URL': GOSUSLUGI_APPOINTMENT_URL,
        'GOSUSLUGI_MEDICAL_EXAM_URL': GOSUSLUGI_MEDICAL_EXAM_URL,
        'GOSUSLUGI_DOCTOR_HOME_URL': GOSUSLUGI_DOCTOR_HOME_URL,
        'GOSUSLUGI_ATTACH_TO_POLYCLINIC_URL': GOSUSLUGI_ATTACH_TO_POLYCLINIC_URL,
        'CONTACT_CENTER_URL': CONTACT_CENTER_URL,
        'MAP_OF_MEDICAL_INSTITUTIONS_URL': MAP_OF_MEDICAL_INSTITUTIONS_URL
    }


# --- УНИВЕРСАЛЬНЫЕ ФУНКЦИИ ---

def anti_duplicate(rate_limit=1.0):
    """Декоратор для защиты от дублирования событий"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            from bot_config import processed_events  # Ленивый импорт
            
            event = args[0] if args else None
            key_id = None

            # Попытка извлечь user_id
            if hasattr(event, 'from_user') and event.from_user and hasattr(event.from_user, 'user_id'):
                key_id = int(event.from_user.user_id)
            
            # Если user_id нет, используем chat_id как идентификатор
            if not key_id:
                if hasattr(event, 'message') and hasattr(event.message, 'recipient'):
                    key_id = int(event.message.recipient.chat_id)
                elif hasattr(event, 'chat_id'):
                    key_id = int(event.chat_id)

            if not key_id:
                return await func(*args, **kwargs)

            current_time = time.time()
            if key_id in processed_events:
                last_time = processed_events[key_id].get('last_time', 0)
                if current_time - last_time < rate_limit:
                    return

            if key_id not in processed_events:
                processed_events[key_id] = {}
            processed_events[key_id]['last_time'] = current_time

            return await func(*args, **kwargs)
        return wrapper
    return decorator


def with_logging_user_context():
    """Декоратор, устанавливающий current user_id для логов maxapi (логгер 'bot')."""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            event = args[0] if args else None
            user_id = None

            if event is not None:
                # Пытаемся взять user_id из from_user
                if hasattr(event, "from_user") and event.from_user and hasattr(event.from_user, "user_id"):
                    try:
                        user_id = int(event.from_user.user_id)
                    except (TypeError, ValueError):
                        user_id = None

                # Если не получилось — используем chat_id/recipient
                if user_id is None:
                    if hasattr(event, "message") and hasattr(event.message, "recipient"):
                        try:
                            user_id = int(event.message.recipient.chat_id)
                        except (TypeError, ValueError):
                            user_id = None
                    elif hasattr(event, "chat_id"):
                        try:
                            user_id = int(event.chat_id)
                        except (TypeError, ValueError):
                            user_id = None

            # Устанавливаем контекст и гарантированно сбрасываем в finally
            clear_logging_user_id()
            if user_id is not None:
                set_logging_user_id(user_id)

            try:
                return await func(*args, **kwargs)
            finally:
                clear_logging_user_id()

        return wrapper

    return decorator


def cleanup_processed_events():
    """Очистка старых записей для экономии памяти"""
    from bot_config import processed_events  # Ленивый импорт
    
    current_time = time.time()
    expired_chats = [
        chat_id for chat_id, data in processed_events.items()
        if current_time - data.get('last_time', 0) > 3600
    ]
    for chat_id in expired_chats:
        del processed_events[chat_id]


def create_keyboard(buttons_config):
    """Универсальная функция создания клавиатуры"""
    if not buttons_config:
        return None

    try:
        formatted_buttons = []
        for row in buttons_config:
            if not row:  # Пропускаем пустые строки
                continue
                
            button_row = []
            for button in row:
                if isinstance(button, dict):
                    # Проверяем наличие обязательных полей
                    if button.get('type') == 'callback':
                        if not button.get('text') or not button.get('payload'):
                            continue
                        btn = CallbackButton(text=button['text'], payload=button['payload'])
                    elif button.get('type') == 'link':
                        if not button.get('text') or not button.get('url'):
                            continue
                        btn = LinkButton(text=button['text'], url=button['url'])
                    elif button.get('type') == 'contact':
                        if not button.get('text'):
                            continue
                        btn = RequestContactButton(text=button['text'])
                    else:
                        continue
                    button_row.append(btn)
                else:
                    # Если это уже готовый объект кнопки (CallbackButton, LinkButton и т.д.)
                    button_row.append(button)
            
            if button_row:
                formatted_buttons.append(button_row)

        if not formatted_buttons:
            return None

        buttons_payload = ButtonsPayload(buttons=formatted_buttons)
        
        # Проверяем, что payload создан успешно
        if not buttons_payload:
            return None
            
        return Attachment(
            type=AttachmentType.INLINE_KEYBOARD,
            payload=buttons_payload
        )
    except Exception as e:
        # Логируем ошибку создания клавиатуры
        log_system_event("keyboard_creation", "error", error=str(e))
        return None


# --- ФУНКЦИИ УПРАВЛЕНИЯ ВЕБХУКАМИ ---

async def get_webhook_subscriptions():
    """Получить список всех вебхук-подписок"""
    from bot_config import MAX_API_BASE_URL, HEADERS  # Ленивый импорт
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{MAX_API_BASE_URL}/subscriptions", headers=HEADERS) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('subscriptions', [])
                else:
                    log_system_event("webhook", "get_subscriptions_failed", status=response.status)
                    return []
    except Exception as e:
        log_system_event("webhook", "get_subscriptions_error", error=str(e))
        return []


async def delete_webhook_subscription(url: str) -> bool:
    """Удалить конкретную вебхук-подписку"""
    from bot_config import MAX_API_BASE_URL, HEADERS  # Ленивый импорт
    
    try:
        import urllib.parse
        encoded_url = urllib.parse.quote(url, safe='')
        delete_url = f"{MAX_API_BASE_URL}/subscriptions?url={encoded_url}"

        async with aiohttp.ClientSession() as session:
            async with session.delete(delete_url, headers=HEADERS) as response:
                if response.status == 200:
                    log_system_event("webhook", "subscription_deleted", url=url)
                    return True
                else:
                    error_text = await response.text()
                    log_system_event("webhook", "delete_subscription_failed", url=url, error=error_text)
                    return False
    except Exception as e:
        log_system_event("webhook", "delete_subscription_error", url=url, error=str(e))
        return False


async def delete_all_webhook_subscriptions() -> bool:
    """Удалить все вебхук-подписки"""
    log_system_event("webhook", "cleanup_started")
    subscriptions = await get_webhook_subscriptions()

    if not subscriptions:
        log_system_event("webhook", "cleanup_completed", message="No subscriptions to delete")
        return True

    log_system_event("webhook", "subscriptions_found", count=len(subscriptions))
    success_count = 0

    for subscription in subscriptions:
        url = subscription.get('url')
        if url:
            success = await delete_webhook_subscription(url)
            if success:
                success_count += 1
            await asyncio.sleep(0.5)

    result = success_count == len(subscriptions)
    if result:
        log_system_event("webhook", "cleanup_completed", deleted_count=success_count)
    else:
        log_system_event("webhook", "cleanup_partial", deleted_count=success_count, total_count=len(subscriptions))

    return result


async def setup_webhook():
    """Настраивает вебхук в зависимости от режима работы"""
    from bot_config import WEBHOOK_MODE, WEBHOOK_URL, bot  # Ленивый импорт

    log_system_event("webhook", "setup_started", mode=WEBHOOK_MODE, url=WEBHOOK_URL)
    cleanup_success = await delete_all_webhook_subscriptions()

    if not cleanup_success:
        log_system_event("webhook", "cleanup_warning", message="Cleanup completed with errors, but continuing")

    try:
        await bot.subscribe_webhook(
            url=WEBHOOK_URL,
            update_types=["message_created", "message_callback", "bot_started"]
        )
        log_system_event("webhook", "setup_completed", url=WEBHOOK_URL, mode=WEBHOOK_MODE)

        final_subscriptions = await get_webhook_subscriptions()
        if final_subscriptions:
            log_system_event("webhook", "setup_verified", count=len(final_subscriptions))
            return True
        else:
            log_system_event("webhook", "setup_failed", message="No subscriptions after setup")
            return False
    except Exception as e:
        log_system_event("webhook", "setup_error", error=str(e))
        return False


# --- ФУНКЦИИ МЕНЮ ---

def create_main_menu_keyboard():
    """Создает клавиатуру главного меню"""
    urls = _get_url_constants()
    buttons_config = [
        [{'type': 'callback', 'text': '📅 Записаться на приём к врачу', 'payload': "start_visit_doctor"}],
        [{'type': 'callback', 'text': '📄 Записаться по направлению', 'payload': "start_visit_referral"}],
        [{'type': 'callback', 'text': '📋 Мои записи к врачу', 'payload': "my_appointments"}],
        [{'type': 'callback', 'text': '📖 Руководство пользователя', 'payload': "get_user_manual"}],
        [{'type': 'callback', 'text': '🔍 Другие возможности', 'payload': "other_options"}]
    ]
    return create_keyboard(buttons_config)


def create_other_options_keyboard():
    """Создает клавиатуру меню 'Другие возможности'"""
    urls = _get_url_constants()
    buttons_config = [
        #[{'type': 'link', 'text': '🏥 Ближайшие гос мед учреждения', 'url': urls['MAP_OF_MEDICAL_INSTITUTIONS_URL']}],
        #[{'type': 'link', 'text': '📞 Единый контакт-центр здравоохранения Севастополя', 'url': urls['CONTACT_CENTER_URL']}],
        [{'type': 'callback', 'text': '🔔 Настройки напоминаний', 'payload': "reminders_settings"}],
        [{'type': 'callback', 'text': '💬 Онлайн чат с поддержкой', 'payload': "support_request"}],
        [{'type': 'callback', 'text': '⬅️ Назад', 'payload': "back_to_main"}]
    ]
    return create_keyboard(buttons_config)


async def send_main_menu(bot_instance: Bot, chat_id: int, greeting_name: str):
    """Отправляет главное меню с приветствием"""
    keyboard = create_main_menu_keyboard()
    await bot_instance.send_message(
        chat_id=chat_id,
        text=f"Здравствуйте, {greeting_name}!\n\nВыберите услугу:",
        attachments=[keyboard] if keyboard else []
    )


async def send_other_options_menu(bot_instance: Bot, chat_id: int):
    """Отправляет меню 'Другие возможности'"""
    urls = _get_url_constants()
    keyboard = create_keyboard([
        #[{'type': 'link', 'text': '🏥 Ближайшие гос мед учреждения', 'url': urls['MAP_OF_MEDICAL_INSTITUTIONS_URL']}],
        #[{'type': 'link', 'text': '📞 Единый контакт-центр здравоохранения Севастополя', 'url': urls['CONTACT_CENTER_URL']}],
        [{'type': 'callback', 'text': '🔔 Настройки напоминаний', 'payload': "reminders_settings"}],
        [{'type': 'callback', 'text': '💬 Онлайн чат с поддержкой', 'payload': "support_request"}],
        [{'type': 'callback', 'text': '⬅️ Назад', 'payload': "back_to_main"}]
    ])
    await bot_instance.send_message(
        chat_id=chat_id,
        text="🔍 Другие возможности:",
        attachments=[keyboard] if keyboard else []
    )


# --- ФОНОВЫЕ ЗАДАЧИ ---

async def make_keepalive_request(session):
    """Периодические запросы для поддержания активности бота"""
    from bot_config import MAX_API_BASE_URL, HEADERS  # Ленивый импорт
    
    try:
        async with session.get(f"{MAX_API_BASE_URL}/me", headers=HEADERS) as response:
            if response.status == 200:
                log_system_event("keepalive", "success", status=response.status)
            else:
                log_system_event("keepalive", "failed", status=response.status,
                                 response_text=await response.text())
    except Exception as e:
        log_system_event("keepalive", "error", error=str(e))


async def keepalive_worker():
    """Фоновая задача для периодических запросов поддержания активности"""
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await make_keepalive_request(session)
                await asyncio.sleep(1800)  # 30 минут
            except asyncio.CancelledError:
                log_system_event("keepalive", "worker_stopped")
                break
            except Exception as e:
                log_system_event("keepalive", "worker_error", error=str(e))
                await asyncio.sleep(300)


async def booking_states_cleanup_worker():
    """Фоновая задача для очистки истекших состояний записи к врачу"""
    from visit_a_doctor.handlers import cleanup_expired_states  # Ленивый импорт
    
    while True:
        try:
            await asyncio.sleep(300)  # Проверяем каждые 5 минут
            cleaned_count = cleanup_expired_states()
            if cleaned_count > 0:
                log_system_event("booking_cleanup", "states_cleaned", count=cleaned_count)
        except asyncio.CancelledError:
            log_system_event("booking_cleanup", "worker_stopped")
            break
        except Exception as e:
            log_system_event("booking_cleanup", "worker_error", error=str(e))
            await asyncio.sleep(300)

async def chat_cleanup_worker():
    """Фоновая задача для очистки чатов поддержки"""
    from bot_config import support_handler  # Ленивый импорт
    
    try:
        await support_handler.start_cleanup_task()
    except Exception as e:
        log_system_event("chat_cleanup", "start_error", error=str(e))


async def send_pending_notifications():
    """Отправляет ожидающие уведомления пользователям и админу"""
    from bot_config import bot, support_handler  # Ленивый импорт
    
    try:
        for user_id, chat_info in list(support_handler.active_chats.items()):
            if 'pending_notification' in chat_info:
                notification = chat_info['pending_notification']
                try:
                    target_chat_id = chat_info.get('chat_id', user_id)
                    try:
                        set_logging_user_id(user_id)
                        await bot.send_message(chat_id=target_chat_id, text=notification)
                    finally:
                        clear_logging_user_id()
                    del chat_info['pending_notification']
                except Exception as e:
                    log_system_event("support_chat", "send_notification_error",
                                     error=str(e), user_id=user_id)

        if hasattr(support_handler, 'admin_notifications'):
            for admin_id, notification in list(support_handler.admin_notifications.items()):
                try:
                    try:
                        set_logging_user_id(admin_id)
                        await bot.send_message(chat_id=admin_id, text=notification)
                    finally:
                        clear_logging_user_id()
                    del support_handler.admin_notifications[admin_id]
                except Exception as e:
                    log_system_event("support_chat", "send_admin_notification_error",
                                     error=str(e), admin_id=admin_id)
    except Exception as e:
        log_system_event("support_chat", "send_notifications_error", error=str(e))


async def notification_worker():
    """Фоновая задача для отправки уведомлений"""
    while True:
        try:
            await send_pending_notifications()
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            break
        except Exception as e:
            log_system_event("notification_worker", "error", error=str(e))
            await asyncio.sleep(5)


async def stop_all_tasks(*tasks):
    """Останавливает все фоновые задачи"""
    tasks_to_cancel = [task for task in tasks if task]

    for task in tasks_to_cancel:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

