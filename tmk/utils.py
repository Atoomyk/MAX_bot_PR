"""
Вспомогательные функции для модуля ТМК
"""
from datetime import datetime
import pytz

# Московская временная зона
MOSCOW_TZ = pytz.timezone('Europe/Moscow')


def normalize_phone(phone: str) -> str:
    """
    Нормализация номера телефона к формату +7XXXXXXXXXX
    
    Args:
        phone: Номер телефона в любом формате
        
    Returns:
        Нормализованный номер телефона
    """
    # Убираем все нечисловые символы
    digits = ''.join(filter(str.isdigit, phone))
    
    # Конвертируем 8 в +7
    if digits.startswith('8') and len(digits) == 11:
        digits = '7' + digits[1:]
    
    # Добавляем +7 если начинается с 9
    if digits.startswith('9') and len(digits) == 10:
        digits = '7' + digits
    
    # Добавляем +
    if not digits.startswith('+'):
        digits = '+' + digits
    
    return digits


def parse_datetime_with_tz(dt_string: str) -> datetime:
    """
    Парсинг datetime строки с timezone в московское время
    
    Args:
        dt_string: Строка формата "2018-12-30T12:00:00.000+0300"
        
    Returns:
        datetime объект в московской timezone
    """
    # Парсим datetime с timezone
    dt = datetime.fromisoformat(dt_string)
    
    # Конвертируем в московское время
    if dt.tzinfo is None:
        dt = MOSCOW_TZ.localize(dt)
    else:
        dt = dt.astimezone(MOSCOW_TZ)
    
    return dt


def format_datetime_russian(dt: datetime) -> tuple[str, str]:
    """
    Форматирование datetime для отображения пользователю
    
    Args:
        dt: datetime объект
        
    Returns:
        Кортеж (дата, время) в формате "30.12.2018", "12:00"
    """
    # Конвертируем в московское время если нужно
    if dt.tzinfo is None:
        dt = MOSCOW_TZ.localize(dt)
    else:
        dt = dt.astimezone(MOSCOW_TZ)
    
    date_str = dt.strftime("%d.%m.%Y")
    time_str = dt.strftime("%H:%M")
    
    return date_str, time_str


def fio_to_short(fio: str) -> str:
    """
    Преобразование полного ФИО в краткий вид «Фамилия И.О.»

    Args:
        fio: ФИО в формате "Фамилия Имя Отчество"

    Returns:
        Краткий вид, например "Иванов С.Б." или "Иванов С." при отсутствии отчества
    """
    if not fio or not fio.strip():
        return fio
    parts = fio.split()
    if len(parts) == 1:
        return parts[0]
    # Фамилия И. или Фамилия И.О.
    initial_name = (parts[1][0] + ".") if parts[1] else ""
    initial_mid = (parts[2][0] + ".") if len(parts) > 2 and parts[2] else ""
    short = parts[0]
    if initial_name:
        short += " " + initial_name + initial_mid
    return short


def get_patient_first_name(fio: str) -> str:
    """
    Извлечение имени из ФИО
    
    Args:
        fio: ФИО в формате "Фамилия Имя Отчество"
        
    Returns:
        Имя и отчество
    """
    parts = fio.split()
    if len(parts) >= 2:
        return f"{parts[1]} {parts[2] if len(parts) > 2 else ''}".strip()
    return fio
