# logging_config.py
import logging
import logging.handlers
import os
import re

# Кастомные уровни логирования
USER_LEVEL = 25
SYSTEM_LEVEL = 24
DATA_LEVEL = 23
SECURITY_LEVEL = 22
TRANSPORT_LEVEL = 21

# Регистрация кастомных уровней
logging.addLevelName(USER_LEVEL, "USER")
logging.addLevelName(SYSTEM_LEVEL, "SYSTEM")
logging.addLevelName(DATA_LEVEL, "DATA")
logging.addLevelName(SECURITY_LEVEL, "SECURITY")
logging.addLevelName(TRANSPORT_LEVEL, "TRANSPORT")


class MaskingFilter(logging.Filter):
    """Фильтр для маскирования персональных данных в логах"""

    def mask_phone(self, phone):
        if phone and len(phone) >= 8:
            return phone[:4] + '*' * (len(phone) - 7) + phone[-3:]
        return phone

    def mask_fio(self, fio):
        if not fio:
            return fio
        parts = fio.split()
        if len(parts) >= 3:
            if len(parts[0]) > 2:
                parts[0] = parts[0][:3] + '***'
            if len(parts[1]) > 1:
                parts[1] = parts[1][:1] + '***'
            if len(parts[2]) > 3:
                parts[2] = '***' + parts[2][-3:]
        return ' '.join(parts)

    def filter(self, record):
        try:
            if hasattr(record, 'msg') and record.msg is not None:
                # В некоторых местах/библиотеках msg может быть не строкой (например, объект Error).
                # Приводим к строке, чтобы re.sub не падал.
                if not isinstance(record.msg, str):
                    record.msg = str(record.msg)
            if hasattr(record, 'msg') and record.msg:
                # Маскирование телефонов
                phone_pattern = r'(\+7\d{10})'
                record.msg = re.sub(phone_pattern,
                                    lambda m: self.mask_phone(m.group(1)),
                                    record.msg)
                # Маскирование ФИО
                fio_pattern = r'([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)'
                record.msg = re.sub(fio_pattern,
                                    lambda m: self.mask_fio(m.group(1)),
                                    record.msg)
        except Exception as e:
            # Логируем ошибку маскирования, но не прерываем логирование
            logging.getLogger(__name__).warning(f"Ошибка маскирования данных: {e}")
        return True


def setup_logging():
    """Настройка системы логирования с кастомными уровнями"""

    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger()
    # Устанавливаем минимальный уровень на самый низкий из используемых (TRANSPORT_LEVEL = 21)
    # чтобы все логи (TRANSPORT, SECURITY, DATA, SYSTEM, USER) записывались
    logger.setLevel(TRANSPORT_LEVEL)

    # Очищаем существующие обработчики
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Форматтер для логов
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Обработчик для файлов
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=os.path.join(log_dir, 'bot.log'),
        when='midnight',
        interval=1,
        backupCount=30,
        encoding='utf-8'
    )
    # Устанавливаем минимальный уровень для файла на TRANSPORT_LEVEL, чтобы все логи записывались
    file_handler.setLevel(TRANSPORT_LEVEL)
    file_handler.setFormatter(formatter)
    file_handler.suffix = '%Y-%m-%d'

    # Обработчик для консоли
    console_handler = logging.StreamHandler()
    # Для консоли можно оставить USER_LEVEL, чтобы не засорять вывод
    console_handler.setLevel(USER_LEVEL)
    console_handler.setFormatter(formatter)

    # Добавляем фильтр маскирования
    masking_filter = MaskingFilter()
    file_handler.addFilter(masking_filter)
    console_handler.addFilter(masking_filter)

    # Добавляем обработчики
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Тестовое сообщение
    logging.log(SYSTEM_LEVEL, "Система логирования инициализирована")


# Словари переводов для логирования
USER_EVENT_TRANSLATIONS = {
    "appointment_cancel_error": {
        "invalid_payload": "Ошибка отмены записи: некорректный идентификатор записи",
        "service_unavailable": "Ошибка отмены записи: сервис недоступен",
        "not_found": "Ошибка отмены записи: запись не найдена",
        "already_cancelled": "Ошибка отмены записи: уже отменена",
        "time_limit_exceeded": "Ошибка отмены записи: превышен лимит времени (более 3 часов)",
        "invalid_confirm_payload": "Ошибка отмены записи: некорректный идентификатор при подтверждении",
        "unknown": "Ошибка отмены записи: неизвестная ошибка"
    },
    "appointment_cancel_confirmation_shown": "Показано подтверждение отмены записи",
    "appointment_cancelled": "Запись отменена",
    "appointment_cancel_failed": "Не удалось отменить запись",
    "appointment_cancel_cancelled": "Отмена записи отменена пользователем",
    "button_pressed": {
        "cancel_appointment_back": "Нажата кнопка «Назад» в меню отмены записи",
        "other_options": "Нажата кнопка «Другие возможности»",
        "default": "Нажата кнопка"
    },
    "message_sent": "Отправлено сообщение",
    "other_options_menu_opened": "Открыто меню «Другие возможности»",
    "appointments_list_viewed": "Просмотрен список записей",
    "appointment_details_viewed": "Просмотрены детали записи",
    "bot_started": "Бот запущен пользователем",
    "bot_started_ignored_duplicate": "Игнорирование дубликата запуска бота",
    "already_registered": "Пользователь уже зарегистрирован",
    "new_user_detected": "Обнаружен новый пользователь",
    "registration_start_clicked": "Нажата кнопка начала регистрации",
    "registration_started": "Начата регистрация",
    "phone_confirmed": "Телефон подтвержден",
    "phone_rejected": "Телефон отклонен",
    "phone_extraction_failed": "Не удалось извлечь телефон",
    "invalid_phone_format": "Неверный формат телефона",
    "invalid_fio_format": "Неверный формат ФИО",
    "invalid_birth_date_format": "Неверный формат даты рождения",
    "fio_correction_requested": "Запрошена коррекция ФИО",
    "fio_input_started": "Начат ввод ФИО",
    "birth_date_correction_requested": "Запрошена коррекция даты рождения",
    "registration_data_confirmed": "Данные регистрации подтверждены",
    "incomplete_data_on_confirmation": "Неполные данные при подтверждении",
    "user_confirmed_registration": "Пользователь подтвердил регистрацию",
    "back_to_main_menu": "Возврат в главное меню",
    "reminders_settings_opened": "Открыты настройки напоминаний",
    "reminders_enabled": "Напоминания включены",
    "reminders_disabled": "Напоминания выключены",
    "reminders_back_clicked": "Нажата кнопка «Назад» в настройках напоминаний",
    "support_chat_requested": "Запрошен чат поддержки",
    "chat_requested": "Чат запрошен",
    "added_to_waiting_queue": "Добавлен в очередь ожидания",
    "chat_created": "Чат создан",
    "admin_connected": "Администратор подключился",
    "chat_ended": "Чат завершен",
    "chat_log_saved": "Лог чата сохранен",
    "chat_log_updated": "Лог чата обновлен",
    "message_ignored_unregistered": "Сообщение проигнорировано (пользователь не зарегистрирован)",
    "visit_doctor_start": "Начат сценарий записи к врачу",
    "visit_doctor_text_input": "Текстовый ввод в сценарии записи",
    "visit_doctor_action": {
        "doc_person_me": "Выбор: Записать себя",
        "doc_person_other": "Выбор: Записать другого",
        "doc_confirm_patient_data": "Подтверждение данных пациента",
        "doc_confirm_booking": "Подтверждение записи",
        "default": "Действие в сценарии записи"
    }
}

SYSTEM_EVENT_TRANSLATIONS = {
    "appointment": {
        "cancelled": "Запись отменена",
        "cancel_failed": "Не удалось отменить запись",
        "cancel_failed_external_status": "Ошибка отмены во внешней системе"
    },
    "bot": {
        "starting": "Запуск бота...",
        "webhook_server_starting": "Запуск вебхук-сервера"
    },
    "database": {
        "user_db_connected": "Подключено к БД пользователей",
        "users_table_initialized": "Таблица пользователей инициализирована",
        "users_table_init_error": "Ошибка инициализации таблицы пользователей",
        "reminders_table_initialized": "Таблица напоминаний инициализирована",
        "reminders_table_init_error": "Ошибка инициализации таблицы напоминаний",
        "db_timeout": "Тайм-аут БД"
    },
    "sync": {
        "service_initialized": "Сервис синхронизации инициализирован",
        "scheduler_started": "Планировщик запущен",
        "init_skipped": "Инициализация синхронизации пропущена",
        "init_error": "Ошибка инициализации синхронизации"
    },
    "keepalive": {
        "worker_started": "Keepalive воркер запущен",
        "worker_stopped": "Keepalive воркер остановлен",
        "success": "Keepalive запрос успешен",
        "failed": "Keepalive запрос не удался",
        "error": "Ошибка Keepalive запроса",
        "worker_error": "Ошибка Keepalive воркера"
    },
    "chat_cleanup": {
        "start_error": "Ошибка запуска очистки чатов"
    },
    "notification_worker": {
        "error": "Ошибка воркера уведомлений"
    },
    "webhook": {
        "mode_direct": "Режим вебхука: Прямой",
        "mode_xtunnel": "Режим вебхука: XTunnel",
        "setup_started": "Настройка вебхука начата",
        "cleanup_started": "Очистка вебхуков начата",
        "cleanup_completed": "Очистка вебхуков завершена",
        "cleanup_partial": "Частичная очистка вебхуков",
        "cleanup_warning": "Предупреждение очистки вебхуков",
        "subscriptions_found": "Найдены подписки",
        "subscription_deleted": "Подписка удалена",
        "delete_subscription_failed": "Не удалось удалить подписку",
        "delete_subscription_error": "Ошибка удаления подписки",
        "setup_completed": "Настройка вебхука завершена",
        "setup_verified": "Настройка вебхука проверена",
        "setup_failed": "Настройка вебхука не удалась",
        "setup_error": "Ошибка настройки вебхука",
        "get_subscriptions_failed": "Не удалось получить подписки",
        "get_subscriptions_error": "Ошибка получения подписок"
    },
    "support_chat": {
        "tickets_dir_created": "Создана директория тикетов",
        "cleanup_error": "Ошибка очистки чатов",
        "cleanup_logs_error": "Ошибка очистки логов",
        "auto_end_no_bot": "Авто-завершение: нет бота",
        "auto_ended": "Авто-завершение чата",
        "old_log_deleted": "Удален старый лог",
        "save_log_error": "Ошибка сохранения лога",
        "send_notification_error": "Ошибка отправки уведомления",
        "send_admin_notification_error": "Ошибка отправки уведомления админу",
        "send_notifications_error": "Ошибка отправки уведомлений",
        "no_admin_id": "ID администратора не установлен",
        "no_bot_for_notification": "Нет бота для уведомления",
        "admin_notified": "Администратор уведомлен о новом чате",
        "notify_admin_error": "Ошибка уведомления администратора",
        "connect_admin_error": "Ошибка подключения администратора",
        "forward_to_admin_error": "Ошибка пересылки сообщения администратору",
        "forward_to_user_error": "Ошибка пересылки сообщения пользователю",
        "chat_command_parse_error": "Ошибка парсинга команды чата",
        "chat_command_error": "Ошибка выполнения команды чата",
        "process_admin_message_error": "Ошибка обработки сообщения администратора",
        "end_notification_user_error": "Ошибка уведомления пользователя о завершении",
        "end_notification_admin_error": "Ошибка уведомления администратора о завершении",
        "end_chat_notifications_error": "Ошибка уведомлений о завершении чата",
        "create_image_attachment_error": "Ошибка создания вложения изображения",
        "send_queued_message_error": "Ошибка отправки сообщения из очереди",
        "send_queue_header_error": "Ошибка отправки заголовка очереди"
    },
    "contact_handler": {
        "processing_failed": "Ошибка обработки контакта"
    },
    "admin_command": {
        "non_admin_attempt": "Попытка выполнения админ-команды не администратором",
        "no_text_in_message": "Нет текста в сообщении админа",
        "unknown_sync_command": "Неизвестная команда синхронизации",
        "sync_command_error": "Ошибка выполнения команды синхронизации",
        "command_received": "Получена админ-команда",
        "command_handled": "Админ-команда обработана",
        "sync_handler_not_available": "Обработчик синхронизации недоступен",
        "handled_by_support": "Обработано как сообщение поддержки"
    },
    "admin_sync": {
        "sync_started": "Ручная синхронизация запущена",
        "sync_already_running": "Синхронизация уже выполняется",
        "sync_completed": "Ручная синхронизация завершена успешно",
        "sync_failed": "Ручная синхронизация завершена с ошибкой",
        "sync_command_exception": "Исключение при выполнении синхронизации",
        "status_requested": "Запрошен статус синхронизации",
        "status_command_exception": "Исключение при запросе статуса",
        "cleanup_started": "Запущена ручная очистка",
        "cleanup_completed": "Ручная очистка завершена",
        "cleanup_failed": "Ручная очистка завершена с ошибкой",
        "cleanup_command_exception": "Исключение при очистке",
        "stats_requested": "Запрошена статистика синхронизации",
        "stats_command_exception": "Исключение при запросе статистики",
        "mock_invalid_command": "Неверная команда мок-синхронизации",
        "mock_started": "Запущена мок-синхронизация",
        "mock_already_running": "Мок-синхронизация уже выполняется",
        "mock_command_exception": "Исключение при мок-синхронизации"
    },
    "admin_callback": {
        "sync_action": "Действие синхронизации",
        "sync_callback_error": "Ошибка админ-callback",
        "sync_callback_received": "Получен админ-callback",
        "sync_callback_handled": "Обработан админ-callback"
    },
     "visit_doctor_module": {
        "context_error": "Ошибка контекста записи к врачу"
    },
    "message_handler_error": {
        "default": "Ошибка обработчика сообщений"
    },
     "message_error_send_failed": {
        "default": "Не удалось отправить сообщение об ошибке"
    },
    "callback_error": {
        "default": "Ошибка обработчика callback"
    },
    "callback_error_send_failed": {
        "default": "Не удалось отправить сообщение об ошибке callback"
    },
    "bot_started": {
        "message_send_failed": "Не удалось отправить приветственное сообщение"
    },
    "appointment_view_error": {
        "invalid_appointment_id": "Некорректный ID записи"
    },
    "phone_retrieval_error": {
        "default": "Ошибка получения телефона"
    }

}

DATA_EVENT_TRANSLATIONS = {
    "confirmation_prepared": "Подготовлено подтверждение данных",
    "registration_completed": "Регистрация завершена",
    "registration_failed": "Регистрация не удалась",
    "phone_extracted": "Телефон извлечен из контакта",
    "phone_missing_on_confirmation": "Телефон отсутствует при подтверждении",
    "incomplete_data_on_confirmation": "Неполные данные при подтверждении",
    "fio_entered": "Введено ФИО",
    "birth_date_entered": "Введена дата рождения",
    "phone_entered": "Введен телефон"
}

SECURITY_EVENT_TRANSLATIONS = {
    "consent_accepted": "Согласие на обработку данных принято",
    "chat_started": "Чат начат (безопасность)"
}


def _translate_user_event(action, **details):
    """Переводит событие пользователя на русский"""
    if action in USER_EVENT_TRANSLATIONS:
        translation = USER_EVENT_TRANSLATIONS[action]

        # Если это словарь (для событий с вариантами)
        if isinstance(translation, dict):
            # Проверяем наличие ключа error или payload в details
            if "error" in details:
                error_key = details.get("error", "unknown")
                if error_key in translation:
                    base_msg = translation[error_key]
                else:
                    base_msg = translation.get("default", action)
            elif "payload" in details:
                payload = details.get("payload", "")
                # Проверяем точное совпадение
                if payload in translation:
                    base_msg = translation[payload]
                # Проверяем начало payload (для cancel_appointment:ID и т.д.)
                elif payload.startswith("cancel_appointment:"):
                    base_msg = "Нажата кнопка «Отменить запись»"
                elif payload.startswith("cancel_appointment_confirm:"):
                    base_msg = "Нажата кнопка «Да» для подтверждения отмены записи"
                # Добавленная логика для visit_doctor_action wildcards
                elif payload.startswith("doc_mo_"):
                    base_msg = "Выбор медицинской организации"
                elif payload.startswith("doc_spec_"):
                    base_msg = "Выбор специальности"
                elif payload.startswith("doc_doc_"):
                    base_msg = "Выбор врача"
                elif payload.startswith("doc_date_"):
                    base_msg = "Выбор даты приема"
                elif payload.startswith("doc_time_"):
                    base_msg = "Выбор времени приема"
                elif payload.startswith("doc_back_"):
                    base_msg = "Навигация: Назад"
                else:
                    base_msg = translation.get("default", action)
            else:
                base_msg = translation.get("default", action)
        else:
            base_msg = translation

        # Формируем детали
        detail_parts = []
        if "appointment_id" in details:
            detail_parts.append(f"ID записи={details['appointment_id']}")
        if "error" in details and action not in ["appointment_cancel_error", "appointment_cancel_failed"]:
            detail_parts.append(f"ошибка: {details['error']}")
        elif "payload" in details and action == "button_pressed":
            # payload уже включен в перевод
            pass
        elif "text" in details:
            detail_parts.append(f"«{details['text']}»")

        if detail_parts:
            return f"{base_msg} ({', '.join(detail_parts)})"
        return base_msg

    # Если перевода нет, возвращаем оригинал
    details_str = " ".join([f'{k}={v}' for k, v in details.items()])
    return f"{action} {details_str}" if details_str else action


def _translate_system_event(component, event, **details):
    """Переводит системное событие на русский"""
    # Особая обработка для ошибок, которые могут быть в верхнем уровне словаря
    if component in SYSTEM_EVENT_TRANSLATIONS:
         # Если component - это ключ ошибки (как message_handler_error)
        val = SYSTEM_EVENT_TRANSLATIONS[component]
        if isinstance(val, dict) and "default" in val and event not in val:
             # Это случай, где component - это название события ошибки (как message_handler_error)
             # А event - это текст ошибки (str(e))
             base_msg = val["default"]
             return f"{base_msg}: {event}"

        component_translations = SYSTEM_EVENT_TRANSLATIONS[component]
        if event in component_translations:
            base_msg = component_translations[event]

            # Формируем детали
            detail_parts = []
            if "appointment_id" in details:
                detail_parts.append(f"ID записи={details['appointment_id']}")
            if "error" in details:
                detail_parts.append(f"ошибка: {details['error']}")
            if "chat_id" in details:
                detail_parts.append(f"chat_id={details['chat_id']}")
            # Добавляем все остальные параметры кроме тех что уже обработали
            for k, v in details.items():
                if k not in ["appointment_id", "error", "chat_id"]:
                    detail_parts.append(f"{k}={v}")

            if detail_parts:
                return f"{base_msg} ({', '.join(detail_parts)})"
            return base_msg

    # Если перевода нет, возвращаем оригинал
    details_str = " ".join([f'{k}={v}' for k, v in details.items()])
    return f"[{component}] {event} {details_str}" if details_str else f"[{component}] {event}"

def _translate_data_event(operation, **details):
    """Переводит событие данных на русский"""
    if operation in DATA_EVENT_TRANSLATIONS:
        base_msg = DATA_EVENT_TRANSLATIONS[operation]
        
        detail_parts = []
        for k, v in details.items():
             detail_parts.append(f"{k}={v}")
        
        if detail_parts:
            return f"{base_msg} ({', '.join(detail_parts)})"
        return base_msg
        
    details_str = " ".join([f'{k}={v}' for k, v in details.items()])
    return f"{operation} {details_str}" if details_str else operation

def _translate_security_event(event, **details):
    """Переводит событие безопасности на русский"""
    if event in SECURITY_EVENT_TRANSLATIONS:
        base_msg = SECURITY_EVENT_TRANSLATIONS[event]
        
        detail_parts = []
        for k, v in details.items():
             detail_parts.append(f"{k}={v}")
        
        if detail_parts:
            return f"{base_msg} ({', '.join(detail_parts)})"
        return base_msg
        
    details_str = " ".join([f'{k}={v}' for k, v in details.items()])
    return f"{event} {details_str}" if details_str else event


# Утилиты для логирования
def log_user_event(user_id, action, **details):
    """Логирует действия пользователя"""
    translated_msg = _translate_user_event(action, **details)
    logging.log(USER_LEVEL, f"[user_id={user_id}] {translated_msg}")


def log_system_event(component, event, **details):
    """Логирует системные события"""
    translated_msg = _translate_system_event(component, event, **details)
    logging.log(SYSTEM_LEVEL, translated_msg)


def log_data_event(user_id, operation, **details):
    """Логирует работу с данными"""
    translated_msg = _translate_data_event(operation, **details)
    logging.log(DATA_LEVEL, f"[user_id={user_id}] {translated_msg}")


def log_security_event(user_id, event, **details):
    """Логирует события безопасности"""
    translated_msg = _translate_security_event(event, **details)
    logging.log(SECURITY_LEVEL, f"[user_id={user_id}] {translated_msg}")


def log_transport_event(method, endpoint, status, **details):
    """Логирует сетевые события"""
    details_str = " ".join([f'{k}={v}' for k, v in details.items()])
    logging.log(TRANSPORT_LEVEL, f"[{method} {endpoint}] status={status} {details_str}")