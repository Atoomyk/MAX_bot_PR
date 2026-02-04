import json
import os
import asyncio
import aiohttp
from typing import List, Dict, Optional
from dotenv import load_dotenv
from logging_config import log_system_event

load_dotenv()

PATIENT_API_URL = os.getenv("PATIENT_API_URL")
PATIENT_API_USER = os.getenv("PATIENT_API_USER")
PATIENT_API_PASSWORD = os.getenv("PATIENT_API_PASSWORD")

# Флаг для отслеживания недоступности сервиса (чтобы не логировать каждую попытку)
_service_unavailable_logged = False

async def get_patients_by_phone(phone: str) -> List[Dict[str, str]]:
    """
    Запрашивает данные пациентов по номеру телефона.
    Возвращает список словарей с нормализованными данными.
    """
    global _service_unavailable_logged
    
    if not PATIENT_API_URL or not PATIENT_API_USER or not PATIENT_API_PASSWORD:
        if not _service_unavailable_logged:
            log_system_event("patient_api", "configuration_missing")
            _service_unavailable_logged = True
        return []

    # Нормализация телефона: API ожидает 10 цифр (без +7/8)
    clean_phone = ''.join(filter(str.isdigit, phone))
    if len(clean_phone) == 11 and (clean_phone.startswith('7') or clean_phone.startswith('8')):
        clean_phone = clean_phone[1:]
    elif len(clean_phone) != 10:
        log_system_event("patient_api", "invalid_phone_format", phone=phone[:4] + "***" + phone[-3:] if len(phone) > 7 else "***")
        return []

    url = f"{PATIENT_API_URL}"
    params = {'phone': clean_phone}
    auth = aiohttp.BasicAuth(login=PATIENT_API_USER, password=PATIENT_API_PASSWORD)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, auth=auth, timeout=10) as response:
                if response.status != 200:
                    log_system_event("patient_api", "http_error", status=response.status, phone=clean_phone[:3] + "***" + clean_phone[-2:])
                    return []
                
                try:
                    # Используем utf-8-sig для обработки BOM
                    text_data = await response.text(encoding='utf-8-sig')
                    data = json.loads(text_data)
                except json.JSONDecodeError as e:
                    log_system_event("patient_api", "parse_error", error=f"JSON decode failed: {str(e)[:100]}")
                    return []
                except Exception as e:
                    log_system_event("patient_api", "parse_error", error=f"Unexpected error: {type(e).__name__}")
                    return []

                if not isinstance(data, list):
                    log_system_event("patient_api", "unexpected_format", data_type=type(data).__name__)
                    return []

                results = []
                for item in data:
                    try:
                        # Склеиваем ФИО
                        last_name = item.get("LastName", "").strip()
                        first_name = item.get("FirstName", "").strip()
                        father_name = item.get("FatherName", "").strip()
                        fio = f"{last_name} {first_name} {father_name}".strip()

                        # Обработка пола "1" - М, "2" - Ж
                        sex_code = item.get("Sex", "")
                        gender = None
                        if sex_code == "1":
                            gender = "Мужской"
                        elif sex_code == "2":
                            gender = "Женский"

                        patient = {
                            "fio": fio,
                            "birth_date": item.get("Birthday", ""),
                            "snils": item.get("Snils", ""),
                            "oms": item.get("PolicyOmsNumber", ""),
                            "gender": gender,
                            # Сохраняем и сырые данные на всякий случай
                            "raw_id": item.get("UniqueId", "") 
                        }
                        results.append(patient)
                    except Exception as parse_error:
                        log_system_event("patient_api", "item_parse_error", error=type(parse_error).__name__)
                        continue
                
                # Сбрасываем флаг при успешном запросе
                if _service_unavailable_logged:
                    _service_unavailable_logged = False
                    log_system_event("patient_api", "service_restored")
                
                return results

    except aiohttp.ClientConnectorError:
        # Ошибка подключения - логируем только один раз
        if not _service_unavailable_logged:
            log_system_event("patient_api", "connection_failed", url=PATIENT_API_URL if PATIENT_API_URL else "not_configured")
            _service_unavailable_logged = True
        return []
    except (aiohttp.ServerTimeoutError, asyncio.TimeoutError):
        # Таймаут запроса
        if not _service_unavailable_logged:
            log_system_event("patient_api", "timeout_error", url=PATIENT_API_URL if PATIENT_API_URL else "not_configured")
            _service_unavailable_logged = True
        return []
    except aiohttp.ClientError as e:
        # Другие ошибки клиента
        log_system_event("patient_api", "client_error", error=type(e).__name__)
        return []
    except Exception as e:
        # Неожиданные ошибки - логируем кратко
        log_system_event("patient_api", "unexpected_error", error=type(e).__name__)
        return []
