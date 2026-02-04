"""
Сервис напоминаний о телемедицинских консультациях
In-memory очередь с восстановлением после рестарта
"""
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Tuple
from queue import PriorityQueue
import pytz

from maxapi import Bot
from maxapi.types import Attachment, CallbackButton, ButtonsPayload
from maxapi.utils.inline_keyboard import AttachmentType

from logging_config import log_system_event
from tmk.database import TelemedDatabase
from tmk.sferum_client import SferumClient
from tmk.message_builder import (
    build_initial_message,
    build_reminder_24h_with_consent,
    build_reminder_24h_without_consent,
    build_reminder_15m_with_link,
    build_reminder_15m_without_consent
)
from tmk.utils import MOSCOW_TZ
from user_database import db as user_db


class ReminderService:
    """Сервис управления напоминаниями о ТМК"""
    
    def __init__(self, bot: Bot, db: TelemedDatabase):
        """
        Args:
            bot: Экземпляр MAX бота
            db: Экземпляр базы данных ТМК
        """
        self.bot = bot
        self.db = db
        self.queue: PriorityQueue = PriorityQueue()
        self.running: bool = False
        self.task: Optional[asyncio.Task] = None
    
    async def start(self):
        """Запуск сервиса напоминаний"""
        if self.running:
            log_system_event("reminder_service", "already_running")
            return
        
        self.running = True
        
        # Загрузка неотправленных напоминаний из БД
        await self._load_pending_reminders()
        
        # Запуск фонового процесса обработки очереди
        self.task = asyncio.create_task(self._process_queue())
        
        log_system_event("reminder_service", "started")
    
    async def stop(self):
        """Остановка сервиса"""
        self.running = False
        
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        
        log_system_event("reminder_service", "stopped")
    
    async def _load_pending_reminders(self):
        """Загрузка неотправленных напоминаний из БД в очередь"""
        log_system_event("reminder_service", "loading_pending_reminders")
        
        sessions = self.db.get_pending_reminders()
        
        loaded_count = 0
        for session in sessions:
            session_id = str(session['id'])
            now = datetime.now(MOSCOW_TZ)
            
            # Добавляем напоминание за 24 часа
            if session['reminder_24h_sent_at'] is None and session['reminder_24h_at']:
                if session['reminder_24h_at'] > now:
                    await self.add_reminder(session_id, '24h', session['reminder_24h_at'])
                    loaded_count += 1
            
            # Добавляем напоминание за 5 минут (в БД хранится в reminder_15m_at)
            if session['reminder_15m_sent_at'] is None and session['reminder_15m_at']:
                if session['reminder_15m_at'] > now:
                    await self.add_reminder(session_id, '15m', session['reminder_15m_at'])
                    loaded_count += 1

            # Добавляем событие «добавить участников»:
            # - врача (admin) добавляем независимо от согласия
            # - пациента добавляем ТОЛЬКО после согласия (consent_at)
            schedule_date = session.get('schedule_date')
            doctor_added_at = session.get('chat_doctor_added_at')
            patient_added_at = session.get('chat_patient_added_at')
            has_consent = session.get('consent_at') is not None
            need_members_add = (doctor_added_at is None) or (has_consent and patient_added_at is None)
            if schedule_date and need_members_add:
                for minutes_before in (15, 5, 2):
                    members_add_at = schedule_date - timedelta(minutes=minutes_before)
                    if members_add_at > now:
                        await self.add_reminder(session_id, 'members_add', members_add_at)
                        loaded_count += 1

            # Добавляем событие «создать звонок» за 2 минуты до консультации
            call_started_at = session.get('call_started_at')
            if schedule_date and call_started_at is None:
                call_start_at = schedule_date - timedelta(minutes=2)
                if call_start_at > now:
                    await self.add_reminder(session_id, 'call_start', call_start_at)
                    loaded_count += 1
        
        log_system_event(
            "reminder_service",
            "reminders_loaded",
            count=loaded_count,
            sessions=len(sessions)
        )
    
    async def add_reminder(self, session_id: str, reminder_type: str, send_at: datetime):
        """
        Добавление напоминания в очередь
        
        Args:
            session_id: UUID сессии
            reminder_type: Тип ('24h', '15m', 'members_add' или 'call_start')
            send_at: Время отправки
        """
        # Преобразуем в timestamp для сортировки в очереди
        timestamp = send_at.timestamp()
        
        # Добавляем в очередь (сортировка по времени)
        await asyncio.get_event_loop().run_in_executor(
            None,
            self.queue.put,
            (timestamp, session_id, reminder_type)
        )
        
        log_system_event(
            "reminder_service",
            "reminder_added",
            session_id=session_id,
            reminder_type=reminder_type,
            send_at=send_at.isoformat()
        )

    def _is_send_success(self, result) -> tuple[bool, str]:
        """
        Определяет успех отправки сообщения в MAX.

        В зависимости от версии maxapi, send_message может:
        - вернуть объект/словарь с полем code (ошибка),
        - вернуть объект класса Error,
        - вернуть успешный объект/словарь без code.

        Returns:
            (ok, error_code_or_reason)
        """
        try:
            if result is None:
                return False, "send_result_none"
            # dict-like
            if isinstance(result, dict):
                code = result.get("code")
                if code:
                    return False, str(code)
                return True, ""
            # object-like
            type_name = type(result).__name__
            code = getattr(result, "code", None)
            if code:
                return False, str(code)
            raw = getattr(result, "raw", None)
            if isinstance(raw, dict) and raw.get("code"):
                return False, str(raw.get("code"))
            if type_name.lower() == "error":
                return False, "Error"
            return True, ""
        except Exception as e:
            return False, f"send_result_inspection_failed:{type(e).__name__}:{e}"
    
    async def _process_queue(self):
        """Основной цикл обработки очереди напоминаний"""
        log_system_event("reminder_service", "queue_processing_started")
        
        while self.running:
            try:
                # Проверяем, есть ли элементы в очереди
                if self.queue.empty():
                    # Ждём 60 секунд перед следующей проверкой
                    await asyncio.sleep(60)
                    continue
                
                # Получаем следующее напоминание (без удаления из очереди)
                timestamp, session_id, reminder_type = await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.queue.queue[0].__getitem__,
                    slice(None)
                )
                
                # Конвертируем timestamp в datetime
                send_at = datetime.fromtimestamp(timestamp, tz=MOSCOW_TZ)
                now = datetime.now(MOSCOW_TZ)
                
                # Вычисляем время ожидания
                wait_seconds = (send_at - now).total_seconds()
                
                if wait_seconds > 60:
                    # Если ждать больше минуты - спим минуту и проверяем снова
                    await asyncio.sleep(60)
                    continue
                
                elif wait_seconds > 0:
                    # Ждём точное время
                    await asyncio.sleep(wait_seconds)
                else:
                    # Если мы слегка опоздали (миллисекунды/секунды) — всё равно выполняем задачу.
                    # Это важно, т.к. из-за планировщика/await sleep(60) мы можем проснуться чуть позже send_at.
                    # Защита от "слишком поздних" действий делается на уровне обработчиков (например, 1 час для напоминаний).
                    if wait_seconds < -3600:
                        # Слишком поздно: удаляем из очереди и пропускаем.
                        await asyncio.get_event_loop().run_in_executor(None, self.queue.get)
                        log_system_event(
                            "reminder_service",
                            "reminder_time_too_old_queue_skip",
                            session_id=session_id,
                            reminder_type=reminder_type,
                            send_at=send_at.isoformat(),
                            now=now.isoformat(),
                            wait_seconds=wait_seconds,
                        )
                        continue
                
                # Удаляем из очереди
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.queue.get
                )
                
                if reminder_type == 'call_start':
                    await self._start_telemed_call(session_id)
                elif reminder_type == 'members_add':
                    await self._add_telemed_chat_members(session_id)
                else:
                    await self._send_reminder(session_id, reminder_type)
                
            except Exception as e:
                log_system_event(
                    "reminder_service",
                    "queue_processing_error",
                    error=str(e)
                )
                await asyncio.sleep(60)
    
    async def _start_telemed_call(self, session_id: str):
        """
        Создание звонка в чате ТМК в момент schedule_date (educationSchool.callStart).

        Args:
            session_id: UUID сессии
        """
        log_system_event(
            "reminder_service",
            "starting_telemed_call",
            session_id=session_id,
        )
        session = self.db.get_session_by_id(session_id)
        if not session:
            log_system_event(
                "reminder_service",
                "call_start_session_not_found",
                session_id=session_id,
            )
            return
        if session["status"] == "CANCELLED":
            log_system_event(
                "reminder_service",
                "call_start_session_cancelled",
                session_id=session_id,
            )
            return
        if session.get("call_started_at") is not None:
            log_system_event(
                "reminder_service",
                "call_start_already_done",
                session_id=session_id,
            )
            return
        chat_id = session.get("chat_id")
        if not chat_id:
            log_system_event(
                "reminder_service",
                "call_start_no_chat_id",
                session_id=session_id,
            )
            return
        call_data = await SferumClient.start_call(chat_id)
        if call_data and call_data.get("join_link"):
            self.db.update_call_started(session_id, join_link=call_data["join_link"])
            log_system_event(
                "reminder_service",
                "call_started_successfully",
                session_id=session_id,
                chat_id=chat_id,
                call_id=call_data.get("call_id"),
            )
        else:
            log_system_event(
                "reminder_service",
                "call_start_failed",
                session_id=session_id,
                chat_id=chat_id,
            )

    async def _add_telemed_chat_members(self, session_id: str):
        """
        Добавление врача и пациента в чат ТМК (educationSchool.addChatUsers).
        Запускается по событию members_add (за 15 минут, с повторами ближе к консультации).

        Args:
            session_id: UUID сессии
        """
        log_system_event(
            "reminder_service",
            "adding_telemed_chat_members",
            session_id=session_id,
        )
        session = self.db.get_session_by_id(session_id)
        if not session:
            log_system_event(
                "reminder_service",
                "members_add_session_not_found",
                session_id=session_id,
            )
            return
        if session["status"] == "CANCELLED":
            log_system_event(
                "reminder_service",
                "members_add_session_cancelled",
                session_id=session_id,
            )
            return
        # Врач добавляется независимо от consent_at, пациент — только при наличии согласия
        doctor_done = session.get("chat_doctor_added_at") is not None
        patient_done = session.get("chat_patient_added_at") is not None
        has_consent = session.get("consent_at") is not None

        if doctor_done and (patient_done or not has_consent):
            log_system_event(
                "reminder_service",
                "members_add_already_done",
                session_id=session_id,
            )
            return
        chat_id = session.get("chat_id")
        if not chat_id:
            log_system_event(
                "reminder_service",
                "members_add_no_chat_id",
                session_id=session_id,
            )
            return
        doctor_phone = session.get("clinic_phone")
        patient_phone = session.get("patient_phone")
        if not doctor_phone:
            log_system_event(
                "reminder_service",
                "members_add_missing_doctor_phone",
                session_id=session_id,
            )
            return

        # 1) Добавляем врача (admin), если ещё не добавлен
        if not doctor_done:
            doctor_ok = await SferumClient.add_doctor_as_admin(
                chat_id=chat_id,
                doctor_phone=doctor_phone,
            )
            if doctor_ok:
                self.db.update_chat_doctor_added(session_id)
                log_system_event(
                    "reminder_service",
                    "doctor_added_successfully",
                    session_id=session_id,
                    chat_id=chat_id,
                )
            else:
                log_system_event(
                    "reminder_service",
                    "doctor_add_failed",
                    session_id=session_id,
                    chat_id=chat_id,
                )

        # 2) Пациента добавляем ТОЛЬКО после согласия
        if not has_consent:
            log_system_event(
                "reminder_service",
                "patient_add_skipped_no_consent",
                session_id=session_id,
                chat_id=chat_id,
                message="Пациент не добавлен в чат, т.к. согласие отсутствует",
            )
            return

        if not patient_phone:
            log_system_event(
                "reminder_service",
                "members_add_missing_patient_phone",
                session_id=session_id,
            )
            return

        if not patient_done:
            patient_ok = await SferumClient.add_patient_as_member(
                chat_id=chat_id,
                patient_phone=patient_phone,
            )
            if patient_ok:
                self.db.update_chat_patient_added(session_id)
                log_system_event(
                    "reminder_service",
                    "patient_added_successfully",
                    session_id=session_id,
                    chat_id=chat_id,
                )
            else:
                log_system_event(
                    "reminder_service",
                    "patient_add_failed",
                    session_id=session_id,
                    chat_id=chat_id,
                )
    
    async def _send_reminder(self, session_id: str, reminder_type: str):
        """
        Отправка напоминания пациенту
        
        Args:
            session_id: UUID сессии
            reminder_type: Тип напоминания ('24h' или '15m')
        """
        log_system_event(
            "reminder_service",
            "sending_reminder",
            session_id=session_id,
            reminder_type=reminder_type
        )
        
        # Получаем данные сессии
        session = self.db.get_session_by_id(session_id)
        
        if not session:
            log_system_event(
                "reminder_service",
                "session_not_found",
                session_id=session_id
            )
            return
        
        # Проверяем статус (не отменена ли консультация)
        if session['status'] == 'CANCELLED':
            log_system_event(
                "reminder_service",
                "session_cancelled_skip",
                session_id=session_id
            )
            return
        
        # Проверяем, не отправлялось ли уже это напоминание
        if session[f'reminder_{reminder_type}_sent_at'] is not None:
            log_system_event(
                "reminder_service",
                "reminder_already_sent",
                session_id=session_id,
                reminder_type=reminder_type
            )
            return
        
        # Проверяем, что время напоминания еще актуально (не прошло более чем на 1 час)
        reminder_time = session[f'reminder_{reminder_type}_at']
        if reminder_time:
            now = datetime.now(MOSCOW_TZ)
            time_diff = (now - reminder_time).total_seconds()
            # Если время прошло более чем на 1 час, не отправляем
            if time_diff > 3600:
                log_system_event(
                    "reminder_service",
                    "reminder_time_too_old",
                    session_id=session_id,
                    reminder_type=reminder_type,
                    reminder_time=reminder_time.isoformat(),
                    now=now.isoformat(),
                    time_diff_seconds=time_diff
                )
                return
        
        # Проверяем наличие user_id (найден ли пациент)
        if not session['user_id']:
            log_system_event(
                "reminder_service",
                "user_not_found",
                session_id=session_id,
                phone=session['patient_phone']
            )
            # TODO: Отправка в чат ТМК как fallback
            return
        
        user_id = session['user_id']
        
        # Формируем сообщение в зависимости от типа напоминания
        sent_ok = False
        if reminder_type == '24h':
            sent_ok = await self._send_24h_reminder(user_id, session)
        elif reminder_type == '15m':
            sent_ok = await self._send_15m_reminder(user_id, session)
        
        if not sent_ok:
            log_system_event(
                "reminder_service",
                "reminder_send_failed",
                session_id=session_id,
                reminder_type=reminder_type,
                user_id=user_id,
            )
            return

        # Обновляем БД (защита от дублей) — только если реально отправили
        success = self.db.update_reminder_sent(session_id, reminder_type)
        
        if success:
            log_system_event(
                "reminder_service",
                "reminder_sent_successfully",
                session_id=session_id,
                reminder_type=reminder_type,
                user_id=user_id
            )
        else:
            log_system_event(
                "reminder_service",
                "reminder_already_sent_by_another_instance",
                session_id=session_id,
                reminder_type=reminder_type
            )
    
    async def _send_24h_reminder(self, user_id: int, session: dict) -> bool:
        """Отправка напоминания за 24 часа"""
        # ВАЖНО: Проверяем актуальное состояние согласия из БД в момент отправки
        # Это гарантирует, что если пациент нажал "Согласен" между добавлением в очередь и отправкой,
        # мы получим актуальные данные
        current_session = self.db.get_session_by_id(str(session['id']))
        if not current_session:
            log_system_event(
                "reminder_service",
                "session_not_found_on_send",
                session_id=str(session['id'])
            )
            return False
        
        # Используем актуальные данные сессии
        has_consent = current_session['consent_at'] is not None
        
        if has_consent:
            message_text = build_reminder_24h_with_consent(current_session)
            # Согласие уже есть, кнопку не показываем
            result = await self.bot.send_message(
                user_id=user_id,
                text=message_text
            )
            ok, code = self._is_send_success(result)
            if not ok:
                log_system_event(
                    "reminder_service",
                    "send_message_failed",
                    user_id=user_id,
                    code=code,
                )
            return ok
        else:
            message_text = build_reminder_24h_without_consent(current_session)
            # Согласия нет, показываем кнопку
            return await self._send_with_consent_button(user_id, current_session, message_text)
    
    async def _send_15m_reminder(self, user_id: int, session: dict) -> bool:
        """Отправка напоминания за 15 минут"""
        # ВАЖНО: Проверяем актуальное состояние согласия из БД в момент отправки
        # Это гарантирует, что если пациент нажал "Согласен" между добавлением в очередь и отправкой,
        # мы получим актуальные данные
        current_session = self.db.get_session_by_id(str(session['id']))
        if not current_session:
            log_system_event(
                "reminder_service",
                "session_not_found_on_send",
                session_id=str(session['id'])
            )
            return False
        
        # Используем актуальные данные сессии
        has_consent = current_session['consent_at'] is not None
        
        if has_consent:
            # Отправляем со ссылкой на чат (согласие есть)
            message_text = build_reminder_15m_with_link(current_session)
            result = await self.bot.send_message(
                user_id=user_id,
                text=message_text
            )
            ok, code = self._is_send_success(result)
            if not ok:
                log_system_event(
                    "reminder_service",
                    "send_message_failed",
                    user_id=user_id,
                    code=code,
                )
            return ok
        else:
            # Отправляем информационное сообщение без ссылки, но С кнопкой "Согласен"
            # согласно ТЗ: если consent_at IS NULL → возможно кнопка "Согласен" (опционально)
            message_text = build_reminder_15m_without_consent(current_session)
            return await self._send_with_consent_button(user_id, current_session, message_text)
    
    async def _send_with_consent_button(self, user_id: int, session: dict, message_text: str) -> bool:
        """
        Отправка сообщения с кнопкой согласия
        
        Args:
            user_id: ID пользователя
            session: Данные сессии
            message_text: Текст сообщения
        """
        session_id = str(session['id'])
        
        # Создаём кнопку согласия
        consent_button = CallbackButton(
            text="Согласен",
            payload=f"tmk_consent_{session_id}"
        )
        
        buttons_attachment = Attachment(
            type=AttachmentType.INLINE_KEYBOARD,
            payload=ButtonsPayload(
                buttons=[[consent_button]]
            )
        )
        
        try:
            result = await self.bot.send_message(
                user_id=user_id,
                text=message_text,
                attachments=[buttons_attachment]
            )
            ok, code = self._is_send_success(result)
            if not ok:
                log_system_event(
                    "reminder_service",
                    "send_message_failed",
                    user_id=user_id,
                    code=code,
                )
            return ok
        except Exception as e:
            log_system_event(
                "reminder_service",
                "send_message_error",
                error=str(e),
                user_id=user_id,
            )
            return False
    
    async def send_initial_message(self, user_id: int, session: dict) -> bool:
        """
        Отправка первого сообщения при создании ТМК
        
        Args:
            user_id: ID пользователя
            session: Данные сессии
        """
        message_text = build_initial_message(session)
        return await self._send_with_consent_button(user_id, session, message_text)
