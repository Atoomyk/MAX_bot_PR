"""
Обработчики callback кнопок согласия пациента на ТМК
"""
from datetime import datetime, timedelta
from maxapi.types import MessageCallback, Attachment, CallbackButton, ButtonsPayload
from maxapi.utils.inline_keyboard import AttachmentType

from logging_config import log_user_event, log_system_event
from tmk.database import TelemedDatabase
from tmk.message_builder import (
    build_late_consent_message,
    build_consent_confirmation_message,
    build_consent_after_consultation_message,
)
from tmk.utils import MOSCOW_TZ
from user_database import db as user_db


async def handle_tmk_consent(event: MessageCallback, bot, db: TelemedDatabase, reminder_service=None):
    """
    Обработка нажатия кнопки 'Согласен' на ТМК
    
    Args:
        event: Событие callback от maxapi
        bot: Экземпляр бота
        db: Экземпляр базы данных ТМК
    
    Returns:
        True если событие обработано, False если нет
    """
    payload = event.callback.payload
    
    # Проверяем, что это наш callback
    if not payload.startswith("tmk_consent_"):
        return False
    
    try:
        user_id = int(event.from_user.user_id)
        
        # Извлекаем session_id из payload
        session_id = payload.split("_")[2]
        
        log_user_event(
            user_id=user_id,
            action="tmk_consent_clicked",
            session_id=session_id
        )
        
        # Получаем данные сессии
        session = db.get_session_by_id(session_id)
        
        if not session:
            log_system_event(
                "tmk_handlers",
                "session_not_found",
                session_id=session_id
            )
            return True
        
        # Проверяем статус консультации
        if session['status'] == 'CANCELLED':
            log_system_event(
                "tmk_handlers",
                "consent_cancelled_session",
                session_id=session_id
            )
            return True
        
        now = datetime.now(MOSCOW_TZ)
        schedule_date = session['schedule_date']
        after_consultation = now > schedule_date
        if after_consultation:
            log_system_event(
                "tmk_handlers",
                "consent_after_consultation",
                session_id=session_id
            )
        
        # Проверяем: не было ли уже согласия
        if session['consent_at'] is not None:
            log_system_event(
                "tmk_handlers",
                "consent_already_given",
                session_id=session_id
            )
            return True
        
        # Обновляем согласие в БД (message_id = None, так как структура event.message не содержит id)
        consent_time = datetime.now(MOSCOW_TZ)
        success = db.update_consent(
            session_id=session_id,
            consent_at=consent_time,
            message_id=None
        )
        
        if not success:
            log_system_event(
                "tmk_handlers",
                "consent_update_failed",
                session_id=session_id
            )
            return True
        
        log_user_event(
            user_id=user_id,
            action="tmk_consent_recorded",
            session_id=session_id,
            consent_at=consent_time.isoformat()
        )

        # После согласия планируем добавление участников в чат не ранее чем за 15 минут до консультации.
        if not after_consultation and reminder_service is not None:
            now_consent = datetime.now(MOSCOW_TZ)
            earliest_add = schedule_date - timedelta(minutes=15)
            if now_consent < earliest_add:
                send_at = earliest_add
            else:
                send_at = now_consent + timedelta(seconds=5)
            try:
                await reminder_service.add_reminder(session_id, 'members_add', send_at)
                log_system_event(
                    "tmk_handlers",
                    "members_add_scheduled_after_consent",
                    session_id=session_id,
                    send_at=send_at.isoformat(),
                )
            except Exception as e:
                log_system_event(
                    "tmk_handlers",
                    "members_add_schedule_error",
                    session_id=session_id,
                    error=str(e),
                )
        elif after_consultation:
            log_system_event(
                "tmk_handlers",
                "members_add_skipped_after_consultation",
                session_id=session_id,
            )
        else:
            log_system_event(
                "tmk_handlers",
                "members_add_not_scheduled_no_service",
                session_id=session_id,
            )
        
        # Предпочитаем chat_id из события (самый надёжный), иначе fallback на БД.
        chat_id = None
        try:
            chat_id = int(event.message.recipient.chat_id)
        except Exception:
            chat_id = user_db.get_last_chat_id(user_id)
        
        # Отправляем подтверждающее сообщение с кнопкой "Главное меню"
        if chat_id:
            # Создаем клавиатуру с кнопкой "Главное меню"
            main_menu_button = CallbackButton(text="Главное меню", payload="main_menu")
            buttons_payload = ButtonsPayload(buttons=[[main_menu_button]])
            keyboard = Attachment(
                type=AttachmentType.INLINE_KEYBOARD,
                payload=buttons_payload
            )
            
            confirmation_text = (
                build_consent_after_consultation_message()
                if after_consultation
                else build_consent_confirmation_message()
            )
            
            await bot.send_message(
                chat_id=chat_id,
                text=confirmation_text,
                attachments=[keyboard]
            )
            
            log_system_event(
                "tmk_handlers",
                "consent_confirmation_sent",
                session_id=session_id,
                user_id=user_id
            )
        
        # Проверяем: прошло ли 15-минутное напоминание?
        if session['reminder_15m_sent_at'] is not None:
            # Напоминание уже отправлено, отправляем ссылку сразу
            message_text = build_late_consent_message(session)
            
            if chat_id:
                await bot.send_message(
                    chat_id=chat_id,
                    text=message_text
                )
            
            log_system_event(
                "tmk_handlers",
                "late_consent_link_sent",
                session_id=session_id,
                user_id=user_id
            )
        
        return True
        
    except Exception as e:
        log_system_event(
            "tmk_handlers",
            "consent_handler_error",
            error=str(e),
            user_id=user_id if 'user_id' in locals() else 0
        )
        return False
