# bot_config.py
"""Конфигурация и инициализация бота"""
import os
from typing import Optional
from dotenv import load_dotenv
from maxapi import Bot, Dispatcher
from maxapi.types import (
    Attachment,
    ButtonsPayload,
    CallbackButton,
    LinkButton,
    RequestContactButton
)
from maxapi.utils.inline_keyboard import AttachmentType

from logging_config import setup_logging, log_system_event

# Настройка логирования ДО импорта других модулей
setup_logging()

from support_chat.support_handler import init_support_handler
from registration_handler import RegistrationHandler
from reminder_handler import ReminderHandler
from sync_appointments.service import SyncService
from sync_appointments.scheduler import SchedulerManager
from commands.sync_command import SyncCommandHandler
from user_database import db

# Загрузка переменных окружения
load_dotenv()
TOKEN = os.getenv("MAXAPI_TOKEN")
WEBHOOK_MODE = os.getenv("WEBHOOK_MODE", "xtunnel")
XTUNNEL_URL = os.getenv("XTUNNEL_URL")
DIRECT_WEBHOOK_URL = os.getenv("DIRECT_WEBHOOK_URL")
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "8083"))

# ID администратора
ADMIN_ID = os.getenv("ADMIN_ID")
if ADMIN_ID:
    ADMIN_ID = int(ADMIN_ID)

# URL внешней системы МИС
MIS_API_URL = os.getenv("MIS_API_URL")

# Настройки для ТМК интеграции
MIS_API_TOKEN = os.getenv("MIS_API_TOKEN")
MIS_CALLBACK_URL = os.getenv("MIS_CALLBACK_URL")
SFERUM_ACCESS_TOKEN = os.getenv("SFERUM_ACCESS_TOKEN")
MIS_API_PORT = int(os.getenv("MIS_API_PORT", "8085"))

# Определение URL вебхука
if WEBHOOK_MODE == "direct" and DIRECT_WEBHOOK_URL:
    WEBHOOK_URL = DIRECT_WEBHOOK_URL
    log_system_event("webhook", "mode_direct", url=WEBHOOK_URL)
else:
    WEBHOOK_URL = XTUNNEL_URL
    log_system_event("webhook", "mode_xtunnel", url=WEBHOOK_URL)

# Инициализация бота и диспетчера
bot = Bot(TOKEN)
dp = Dispatcher()

# Константы API
MAX_API_BASE_URL = "https://platform-api.max.ru"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"{TOKEN}"
}

# Ссылки для кнопок главного меню
GOSUSLUGI_APPOINTMENT_URL = "https://www.gosuslugi.ru/10700"
GOSUSLUGI_MEDICAL_EXAM_URL = "https://www.gosuslugi.ru/647521/1/form"
GOSUSLUGI_DOCTOR_HOME_URL = "https://www.gosuslugi.ru/600361"
GOSUSLUGI_ATTACH_TO_POLYCLINIC_URL = "https://www.gosuslugi.ru/600360"
CONTACT_CENTER_URL = "https://sevmiac.ru/ekc/"
MAP_OF_MEDICAL_INSTITUTIONS_URL = "https://yandex.ru/maps/959/sevastopol/search/%D0%91%D0%BE%D0%BB%D1%8C%D0%BD%D0%B8%D1%86%D1%8B%20%D0%B2%20%D1%81%D0%B5%D0%B2%D0%B0%D1%81%D1%82%D0%BE%D0%BF%D0%BE%D0%BB%D0%B5/?ll=33.567033%2C44.573119&sctx=ZAAAAAgCEAAaKAoSCadZoN0hw0BAEUnXTL7ZTkZAEhIJUHEceLVc5j8RKsdkcf8R6D8iBgABAgMEBSgKOABAvwdIAWoCcnWdAc3MzD2gAQCoAQC9AUiRBS%2FCAYoBiNKFmATv5uOzBJjPl5qAAo%2BevdYEwZ%2Bw4gPU7PqeBOi14pEEwauvqgS8ib%2FOiAW%2F3bm7BLiO%2FskE%2FdajkLUCkJjwtQaq8ezXBtbjiYLaBZzM9ssGr8ub4MIEx%2BiRm5oD4P2F1MoDrPT1i9gGktWn1IYBtvLJkM0El4aU98IEiuHzlv8G14e%2Fr%2BkGggIq0JHQvtC70YzQvdC40YbRiyDQsiDRgdC10LLQsNGB0YLQvtC%2F0L7Qu9C1igIsMTg0MTA1OTU2JDE4NDEwNTk1OCQ1MzQzNzI2MDU1OSQxOTgzOTUyODk1NDKSAgM5NTmaAgxkZXNrdG9wLW1hcHOqAgwxNjU3NDI5MTg5Mzk%3D&sll=33.567033%2C44.573119&sspn=0.364266%2C0.147111&z=12.4"

# Глобальные переменные
user_states = {}
processed_events = {}

# Глобальные переменные для синхронизации записей
sync_service: Optional[SyncService] = None
sync_command_handler: Optional[SyncCommandHandler] = None
scheduler_manager: Optional[SchedulerManager] = None

# Глобальные переменные для ТМК
tmk_database = None
tmk_reminder_service = None
tmk_app = None
tmk_bot = None


def init_sync_service():
    """Инициализирует сервис синхронизации записей"""
    global sync_service, sync_command_handler, scheduler_manager

    try:
        if MIS_API_URL and ADMIN_ID:
            sync_service = SyncService(db, bot, MIS_API_URL)
            scheduler_manager = SchedulerManager(sync_service)
            sync_command_handler = SyncCommandHandler(sync_service, int(ADMIN_ID))
            log_system_event("sync", "service_initialized", url=MIS_API_URL)
        else:
            reason = ""
            if not MIS_API_URL:
                reason += "MIS_API_URL отсутствует "
            if not ADMIN_ID:
                reason += "ADMIN_ID отсутствует "
            log_system_event("sync", "init_skipped", reason=reason.strip())
    except Exception as e:
        log_system_event("sync", "init_error", error=str(e))


def init_tmk_service():
    """Инициализирует сервис телемедицинских консультаций"""
    global tmk_database, tmk_reminder_service, tmk_app
    
    try:
        from tmk.database import TelemedDatabase
        from tmk.reminder_service import ReminderService
        from tmk.api import create_tmk_app
        
        # Инициализация базы данных ТМК
        tmk_database = TelemedDatabase(db.conn)
        log_system_event("tmk", "database_initialized")
        
        # Инициализация сервиса напоминаний
        tmk_reminder_service = ReminderService(bot, tmk_database)
        log_system_event("tmk", "reminder_service_initialized")
        
        # Создание FastAPI приложения
        tmk_app = create_tmk_app(bot, tmk_database, tmk_reminder_service)
        log_system_event("tmk", "api_initialized", port=MIS_API_PORT)
        
    except Exception as e:
        log_system_event("tmk", "init_error", error=str(e))


# Инициализация обработчиков
support_handler = init_support_handler(user_states)
registration_handler = RegistrationHandler(user_states)
reminder_handler = ReminderHandler(db, None)

# Устанавливаем бот в обработчики
support_handler.set_bot(bot)


