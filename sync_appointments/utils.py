# sync_appointments/utils.py
"""
Вспомогательные функции для синхронизации записей.
"""

import re
import logging
from datetime import datetime
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)


def normalize_phone(phone_str: str) -> List[str]:
    """
    Нормализует телефонные номера из строки.

    Args:
        phone_str: Строка с телефоном/телефонами (например: "+7(978)550-49-88;+7(978)747-85-55")

    Returns:
        Список нормализованных номеров в формате +7XXXXXXXXXX
    """
    if not phone_str:
        return []

    normalized_numbers = []

    # Разделяем строку по разделителям ; или ,
    phone_parts = re.split(r'[;,]+', phone_str)

    for phone in phone_parts:
        phone = phone.strip()
        if not phone:
            continue

        # Удаляем все символы, кроме + и цифр
        cleaned = re.sub(r'[^\d+]', '', phone)

        if not cleaned:
            continue

        # Приводим к формату +7XXXXXXXXXX
        if cleaned.startswith('+7'):
            normalized = cleaned[:12]  # Берем первые 12 символов (+7 + 10 цифр)
        elif cleaned.startswith('7'):
            normalized = '+7' + cleaned[1:12]  # Заменяем первую 7 на +7
        elif cleaned.startswith('8'):
            normalized = '+7' + cleaned[1:12]  # Заменяем первую 8 на +7
        else:
            # Если номер начинается не с +7, 7 или 8, пропускаем
            logger.warning(f"Неподдерживаемый формат телефона: {phone}")
            continue

        # Проверяем длину (должно быть 12 символов: +7 + 10 цифр)
        if len(normalized) == 12 and normalized[1:].isdigit():
            normalized_numbers.append(normalized)
        else:
            logger.warning(f"Некорректная длина телефона после нормализации: {normalized}")

    return normalized_numbers


def normalize_birth_date(birth_date_str: str) -> Optional[str]:
    """
    Нормализует дату рождения из строки.

    Args:
        birth_date_str: Строка с датой рождения (например: "1978-08-20T00:00:00+03:00")

    Returns:
        Дата в формате YYYY-MM-DD или None если некорректна
    """
    if not birth_date_str:
        return None

    try:
        # Пытаемся извлечь дату из строки (часть до 'T')
        date_part = birth_date_str.split('T')[0]

        # Проверяем формат YYYY-MM-DD
        datetime.strptime(date_part, '%Y-%m-%d')
        return date_part
    except (ValueError, IndexError) as e:
        logger.warning(f"Ошибка нормализации даты рождения '{birth_date_str}': {e}")
        return None


def normalize_fio(last_name: str, first_name: str, middle_name: str) -> str:
    """
    Нормализует ФИО для сравнения.

    Args:
        last_name: Фамилия
        first_name: Имя
        middle_name: Отчество

    Returns:
        ФИО в верхнем регистре
    """
    # Убираем лишние пробелы и объединяем
    fio = f"{last_name or ''} {first_name or ''} {middle_name or ''}"
    fio = ' '.join(fio.split())  # Убираем множественные пробелы

    return fio.upper().strip()


def parse_datetime(datetime_str: str) -> Optional[datetime]:
    """
    Парсит строку с датой-временем.

    Args:
        datetime_str: Строка с датой-временем

    Returns:
        Объект datetime или None если некорректна
    """
    if not datetime_str:
        return None

    try:
        # Пытаемся распарсить различные форматы
        formats = [
            '%Y-%m-%dT%H:%M:%S%z',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%d.%m.%Y %H:%M:%S'
        ]

        for fmt in formats:
            try:
                return datetime.strptime(datetime_str, fmt)
            except ValueError:
                continue

        logger.warning(f"Не удалось распарсить дату: {datetime_str}")
        return None
    except Exception as e:
        logger.warning(f"Ошибка парсинга даты '{datetime_str}': {e}")
        return None


def is_within_allowed_hours(check_time: datetime = None) -> bool:
    """
    Проверяет, находится ли время в разрешенном диапазоне для отправки уведомлений.

    Args:
        check_time: Время для проверки (по умолчанию текущее)

    Returns:
        True если можно отправлять уведомления (08:00-21:00)
    """
    if check_time is None:
        check_time = datetime.now()

    hour = check_time.hour
    # Разрешаем с 8:00 до 21:00
    return 8 <= hour < 22


def format_appointment_for_user(appointment_data: dict) -> str:
    """
    Форматирует информацию о записи для показа пользователю.

    Args:
        appointment_data: Словарь с данными о записи

    Returns:
        Отформатированная строка
    """
    try:
        visit_time = parse_datetime(appointment_data.get('Дата записи', ''))
        if visit_time:
            date_str = visit_time.strftime('%d.%m.%Y')
            time_str = visit_time.strftime('%H:%M')
            datetime_str = f"{date_str} в {time_str}"
        else:
            datetime_str = "не указано"

        lines = [
            f"📅 Запись к врачу:",
            f"• Дата и время: {datetime_str}",
            f"• Мед. учреждение: {appointment_data.get('Мед учреждение', 'не указано')}",
            f"• Адрес: {appointment_data.get('Адрес мед учреждения', 'не указан')}",
        ]
        room = appointment_data.get('Room')
        if room and str(room).strip():
            lines.append(f"• Место приёма: {room.strip()}")
        lines.extend([
            f"• Врач: {appointment_data.get('ФИО врача', 'не указан')}",
            f"• Должность: {appointment_data.get('Должность врача', 'не указана')}"
        ])
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"Ошибка форматирования записи для пользователя: {e}")
        return "Информация о записи временно недоступна."


def extract_doctor_info(specialist_name: str) -> Tuple[str, str]:
    """
    Извлекает ФИО врача и должность из строки Specialist_Name.

    Args:
        specialist_name: Строка вида "Караяни Я.Н. (Дерматовенеролог)"

    Returns:
        Кортеж (ФИО врача, должность)
    """
    if not specialist_name:
        return "", ""

    try:
        # Ищем текст в скобках - это должность
        position_match = re.search(r'\((.*?)\)', specialist_name)
        position = position_match.group(1) if position_match else ""

        # Убираем должность из строки - остаётся ФИО
        doctor_fio = re.sub(r'\s*\(.*?\)\s*', '', specialist_name).strip()

        return doctor_fio, position
    except Exception as e:
        logger.warning(f"Ошибка извлечения информации о враче из '{specialist_name}': {e}")
        return specialist_name, ""