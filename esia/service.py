"""
Сервис для работы с ЕСИА авторизацией
Обработка файлов с данными пользователей после авторизации через ЕСИА
"""
import os
import asyncio
from typing import Optional, Dict
from datetime import datetime
from dotenv import load_dotenv
from logging_config import log_system_event, log_data_event, log_user_event
from user_database import db

load_dotenv()

# Путь к папке с файлами ЕСИА на сервере (из .env)
ESIA_FILES_PATH = os.getenv("ESIA_FILES_PATH")

# Базовые URL ЕСИА (из .env)
ESIA_LOGIN_BASE = os.getenv("ESIA_LOGIN_BASE")
ESIA_CALLBACK_BASE = os.getenv("ESIA_CALLBACK_BASE")

# Количество попыток проверки файла
MAX_CHECK_ATTEMPTS = int(os.getenv("ESIA_MAX_CHECK_ATTEMPTS", "20"))

# Интервал между проверками (в секундах)
CHECK_INTERVAL = int(os.getenv("ESIA_CHECK_INTERVAL", "6"))


def generate_esia_url(user_id: int) -> str:
    """
    Генерирует URL для авторизации через ЕСИА
    
    Args:
        user_id: ID пользователя
        
    Returns:
        URL для авторизации через ЕСИА
    """
    return f"{ESIA_LOGIN_BASE}/cas/login?service={ESIA_CALLBACK_BASE}/auth/cascallback?user_id={user_id}"


def get_esia_file_path(user_id: int) -> str:
    """
    Возвращает путь к файлу ЕСИА для пользователя
    
    Args:
        user_id: ID пользователя
        
    Returns:
        Полный путь к файлу
    """
    filename = f"{user_id}.txt"
    return os.path.join(ESIA_FILES_PATH, filename)



async def check_esia_file(user_id: int) -> Optional[str]:
    """
    Проверяет наличие файла ЕСИА для пользователя
    
    Args:
        user_id: ID пользователя
        
    Returns:
        Путь к файлу, если он найден, иначе None
    """
    file_path = get_esia_file_path(user_id)
    
    if os.path.exists(file_path):
        log_system_event("esia", "file_found", user_id=user_id, file_path=file_path)
        return file_path
    
    return None


async def wait_for_esia_file(user_id: int) -> Optional[str]:
    """
    Ожидает появления файла ЕСИА с повторными проверками
    
    Args:
        user_id: ID пользователя
        
    Returns:
        Путь к файлу, если он появился, иначе None
    """
    log_system_event("esia", "waiting_for_file_start", user_id=user_id, attempts=MAX_CHECK_ATTEMPTS)
    
    for attempt in range(1, MAX_CHECK_ATTEMPTS + 1):
        file_path = await check_esia_file(user_id)
        
        if file_path:
            log_system_event("esia", "file_appeared", user_id=user_id, attempt=attempt)
            return file_path
        
        if attempt < MAX_CHECK_ATTEMPTS:
            await asyncio.sleep(CHECK_INTERVAL)
            log_system_event("esia", "file_check_attempt", user_id=user_id, attempt=attempt)
    
    log_system_event("esia", "file_not_found_after_attempts", user_id=user_id, attempts=MAX_CHECK_ATTEMPTS)
    return None


def parse_esia_file(file_path: str, fallback_phone: Optional[str] = None) -> Optional[Dict[str, str]]:
    """
    Парсит данные из файла ЕСИА
    
    Формат файла: ФИО,телефон,дата_рождения,СНИЛС,ОМС,пол
    Пример: Иван Максим Валерьевич,9787229457,1984-12-13,18122204630,8650910446000105,1
    Поле телефон может быть null — тогда используется fallback_phone (телефон, подтверждённый при регистрации).
    
    Args:
        file_path: Путь к файлу
        fallback_phone: Телефон из регистрации, если в файле указан null
        
    Returns:
        Словарь с данными пользователя или None в случае ошибки
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        
        log_system_event("esia", "file_read", file_path=file_path, content_length=len(content))
        
        # Разделяем по запятой
        parts = [part.strip() for part in content.split(',')]
        
        if len(parts) != 6:
            log_system_event("esia", "file_parse_error", 
                           error=f"Invalid format: expected 6 parts, got {len(parts)}",
                           file_path=file_path)
            return None
        
        fio = parts[0]
        phone_raw = parts[1]
        birth_date_raw = parts[2]  # Формат: 1984-12-13 или null
        snils = parts[3]
        oms = parts[4] if parts[4].lower() != 'null' else None
        gender_code = parts[5]
        
        # Преобразование телефона: добавляем +7. Если в файле null — берём из регистрации
        if phone_raw and str(phone_raw).lower() != 'null' and len(str(phone_raw)) == 10:
            phone = f"+7{phone_raw}"
        elif fallback_phone:
            phone = fallback_phone
        else:
            log_system_event("esia", "file_parse_error", 
                           error=f"Invalid phone format: {phone_raw}",
                           file_path=file_path)
            return None
        
        # Дата рождения обязательна; null не допускается
        if not birth_date_raw or str(birth_date_raw).strip().lower() == 'null':
            log_system_event("esia", "file_parse_error",
                           error="birth_date is null or empty",
                           file_path=file_path)
            return None
        
        # Преобразование даты: 1984-12-13 -> 13.12.1984
        try:
            birth_date_obj = datetime.strptime(birth_date_raw, "%Y-%m-%d")
            birth_date = birth_date_obj.strftime("%d.%m.%Y")
        except ValueError as e:
            log_system_event("esia", "file_parse_error", 
                           error=f"Invalid date format: {birth_date_raw}, {str(e)}",
                           file_path=file_path)
            return None
        
        # Преобразование пола: 1 -> Мужской, 2 -> Женский
        if gender_code == "1":
            gender = "Мужской"
        elif gender_code == "2":
            gender = "Женский"
        else:
            log_system_event("esia", "file_parse_error", 
                           error=f"Invalid gender code: {gender_code}",
                           file_path=file_path)
            return None
        
        data = {
            'fio': fio,
            'phone': phone,
            'birth_date': birth_date,
            'snils': snils,
            'oms': oms,
            'gender': gender
        }
        
        log_system_event("esia", "file_parsed_successfully", 
                        user_fio=fio, 
                        phone=phone[:4] + "***" + phone[-3:] if len(phone) > 7 else "***",
                        birth_date=birth_date)
        
        return data
        
    except FileNotFoundError:
        log_system_event("esia", "file_not_found", file_path=file_path)
        return None
    except Exception as e:
        log_system_event("esia", "file_read_error", 
                        error=str(e), 
                        error_type=type(e).__name__,
                        file_path=file_path)
        return None


def save_esia_data_to_db(user_id: int, chat_id: int, data: Dict[str, str]) -> bool:
    """
    Сохраняет данные из ЕСИА в таблицу users
    
    Если пользователь уже зарегистрирован, данные перезаписываются.
    
    Args:
        user_id: ID пользователя
        chat_id: ID чата
        data: Словарь с данными пользователя
        
    Returns:
        True если данные успешно сохранены, False в случае ошибки
    """
    try:
        fio = data['fio']
        phone = data['phone']
        birth_date = data['birth_date']
        snils = data.get('snils')
        oms = data.get('oms')
        gender = data.get('gender')
        
        # Проверяем, зарегистрирован ли пользователь
        is_registered = db.is_user_registered(user_id)
        
        if is_registered:
            # Обновляем существующие данные
            log_user_event(user_id, "esia_data_update_attempt")
            success = db.update_user_data(user_id, fio, birth_date, snils, oms, gender)
            if success:
                # Обновляем также chat_id и телефон, если нужно
                db.update_last_chat_id(user_id, chat_id)
                log_data_event(user_id, "esia_data_updated", 
                             fio=fio, 
                             phone=phone[:4] + "***" + phone[-3:] if len(phone) > 7 else "***",
                             status="success")
                return True
            else:
                log_data_event(user_id, "esia_data_update_failed", 
                             fio=fio, 
                             phone=phone[:4] + "***" + phone[-3:] if len(phone) > 7 else "***",
                             status="failed")
                return False
        else:
            # Регистрируем нового пользователя
            log_user_event(user_id, "esia_data_registration_attempt")
            success = db.register_user(user_id, chat_id, fio, phone, birth_date, snils, oms, gender)
            if success:
                log_data_event(user_id, "esia_data_registered", 
                             fio=fio, 
                             phone=phone[:4] + "***" + phone[-3:] if len(phone) > 7 else "***",
                             status="success")
                return True
            else:
                log_data_event(user_id, "esia_data_registration_failed", 
                             fio=fio, 
                             phone=phone[:4] + "***" + phone[-3:] if len(phone) > 7 else "***",
                             status="failed")
                return False
                
    except Exception as e:
        log_system_event("esia", "db_save_error", 
                        error=str(e), 
                        error_type=type(e).__name__,
                        user_id=user_id)
        return False


def delete_esia_file(file_path: str) -> bool:
    """
    Удаляет файл ЕСИА после обработки
    
    Args:
        file_path: Путь к файлу
        
    Returns:
        True если файл успешно удален, False в случае ошибки
    """
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            log_system_event("esia", "file_deleted", file_path=file_path)
            return True
        else:
            log_system_event("esia", "file_not_found_for_deletion", file_path=file_path)
            return False
    except Exception as e:
        log_system_event("esia", "file_deletion_error", 
                        error=str(e), 
                        error_type=type(e).__name__,
                        file_path=file_path)
        return False
