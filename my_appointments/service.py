from datetime import datetime
from typing import Optional, List, Dict, Any

from maxapi import Bot

from logging_config import log_system_event, log_user_event
from sync_appointments.database import AppointmentsDatabase
from sync_appointments.utils import format_appointment_for_user
from user_database import db
from bot_utils import create_keyboard


_appointments_db: Optional[AppointmentsDatabase] = None


def _get_appointments_db() -> Optional[AppointmentsDatabase]:
    """
    Возвращает экземпляр AppointmentsDatabase.
    Сначала пробует взять из sync_service, иначе создаёт собственный на основе user_database.db.
    """
    global _appointments_db

    if _appointments_db is not None:
        return _appointments_db

    # Пытаемся использовать уже инициализированный sync_service, если он есть
    try:
        from bot_config import sync_service  # ленивый импорт, чтобы избежать циклов

        if sync_service and getattr(sync_service, "appointments_db", None):
            _appointments_db = sync_service.appointments_db
            return _appointments_db
    except Exception as e:
        log_system_event("my_appointments", "sync_service_access_failed", error=str(e))

    # Фоллбек: создаём локальный экземпляр AppointmentsDatabase поверх user_database.db
    try:
        _appointments_db = AppointmentsDatabase(db)
        return _appointments_db
    except Exception as e:
        log_system_event("my_appointments", "appointments_db_init_failed", error=str(e))
        return None


def _filter_and_sort_appointments(
    appointments: List[Dict[str, Any]],
    now: datetime,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """
    Оставляет только будущие записи (начиная с now), сортирует по времени и ограничивает лимитом.
    """
    future: List[Dict[str, Any]] = []
    for app in appointments:
        visit_time = app.get("visit_time")
        if not visit_time:
            continue
        try:
            if visit_time >= now:
                future.append(app)
        except TypeError:
            # На случай несовпадающих типов datetime, безопасно пропускаем
            continue

    future.sort(key=lambda a: a.get("visit_time") or datetime.max)
    return future[:limit]


async def send_my_appointments(bot: Bot, user_id: int, chat_id: int) -> None:
    """
    Отправляет пользователю список его ближайших записей к врачу
    с возможностью отмены и переходом в главное меню.
    """
    appointments_db = _get_appointments_db()
    if not appointments_db:
        await bot.send_message(
            chat_id=chat_id,
            text="Сервис записей временно недоступен. Пожалуйста, попробуйте позже.",
        )
        return

    now = datetime.now()

    try:
        # Берём с запасом, затем фильтруем и ограничиваем
        raw_appointments = appointments_db.get_user_appointments(user_id, limit=50)
    except Exception as e:
        log_system_event(
            "my_appointments",
            "get_user_appointments_failed",
            error=str(e),
            user_id=user_id,
        )
        await bot.send_message(
            chat_id=chat_id,
            text="Сервис записей временно недоступен. Пожалуйста, попробуйте позже.",
        )
        return

    future_appointments = _filter_and_sort_appointments(raw_appointments, now, limit=5)

    if not future_appointments:
        keyboard = create_keyboard(
            [[{"type": "callback", "text": "🏠 Главное меню", "payload": "back_to_main"}]]
        )
        await bot.send_message(
            chat_id=chat_id,
            text=(
                "Нет данных по вашим записям! "
                "Записи, сделанные через другие сервисы доступны за день до приёма к врачу!"
            ),
            attachments=[keyboard] if keyboard else [],
        )
        log_user_event(user_id, "my_appointments_empty")
        return

    user_data = db.get_user_full_data(user_id) or {}
    user_fio = (user_data.get("fio") or "").strip()

    parts: List[str] = []
    for idx, app in enumerate(future_appointments, start=1):
        data = app.get("data") or {}
        base_text = format_appointment_for_user(data)

        # Определяем пациента
        patient_fio = ""
        original_block = data.get("Исходные_данные") or {}
        if isinstance(original_block, dict):
            patient_fio = (original_block.get("ФИО пациента") or "").strip()

        extra_patient = ""
        if patient_fio and user_fio and patient_fio != user_fio:
            extra_patient = f"\n👤 Пациент: {patient_fio}"

        header = f"Запись #{idx}\n"
        parts.append(header + base_text + extra_patient)

    message_text = "📋 Ваши ближайшие записи к врачу:\n\n" + "\n\n".join(parts)

    # Формируем клавиатуру: кнопки отмены + кнопка "Главное меню"
    button_rows: List[List[Dict[str, str]]] = []
    multiple = len(future_appointments) > 1

    for idx, app in enumerate(future_appointments, start=1):
        app_id = app.get("id")
        if not app_id:
            continue
        if multiple:
            btn_text = f"❌ Отменить запись #{idx}"
        else:
            btn_text = "❌ Отменить запись"
        button_rows.append(
            [
                {
                    "type": "callback",
                    "text": btn_text,
                    "payload": f"cancel_appointment:{app_id}",
                }
            ]
        )

    button_rows.append(
        [
            {
                "type": "callback",
                "text": "🏠 Главное меню",
                "payload": "back_to_main",
            }
        ]
    )

    keyboard = create_keyboard(button_rows)

    await bot.send_message(
        chat_id=chat_id,
        text=message_text,
        attachments=[keyboard] if keyboard else [],
    )
    log_user_event(
        user_id,
        "my_appointments_shown",
        count=len(future_appointments),
    )

