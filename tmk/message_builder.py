"""
Формирование текстов сообщений для пациентов
"""
from datetime import datetime
from typing import Dict, Any
from tmk.utils import format_datetime_russian, get_patient_first_name


def build_initial_message(session: Dict[str, Any]) -> str:
    """
    Первое сообщение при создании ТМК с запросом согласия
    
    Args:
        session: Данные сессии из БД
        
    Returns:
        Текст сообщения
    """
    date_str, time_str = format_datetime_russian(session['schedule_date'])
    
    message = f"""Вы записаны на телемедицинскую консультацию.

МО: {session['clinic_name']}
Врач: {session['doctor_fio']}
Специальность: {session['doctor_specialization']}
Дата: {date_str}
Время: {time_str} (Московское время)

За 1 день и за 5 минут до консультации придёт напоминание в этот чат."""
    
    return message


def build_reminder_24h_without_consent(session: Dict[str, Any]) -> str:
    """
    Напоминание за 24 часа если согласия ещё нет
    
    Args:
        session: Данные сессии из БД
        
    Returns:
        Текст сообщения
    """
    date_str, time_str = format_datetime_russian(session['schedule_date'])
    
    message = f"""Вы записаны на телемедицинскую консультацию.

МО: {session['clinic_name']}
Врач: {session['doctor_fio']}
Специальность: {session['doctor_specialization']}
Дата: {date_str}
Время: {time_str} (Московское время)

За 5 минут до консультации придёт напоминание в этот чат и ссылка на чат телемед консультации в случае вашего согласия."""
    
    return message


def build_reminder_24h_with_consent(session: Dict[str, Any]) -> str:
    """
    Напоминание за 24 часа если согласие уже есть
    
    Args:
        session: Данные сессии из БД
        
    Returns:
        Текст сообщения
    """
    date_str, time_str = format_datetime_russian(session['schedule_date'])
    
    message = f"""Вы записаны на телемедицинскую консультацию.

МО: {session['clinic_name']}
Врач: {session['doctor_fio']}
Специальность: {session['doctor_specialization']}
Дата: {date_str}
Время: {time_str} (Московское время)

За 5 минут до консультации придёт напоминание в этот чат и ссылка на чат телемед консультации."""
    
    return message


def build_reminder_15m_with_link(session: Dict[str, Any]) -> str:
    """
    Напоминание за 5 минут со ссылкой на чат (только если есть согласие)
    
    Args:
        session: Данные сессии из БД
        
    Returns:
        Текст сообщения
    """
    patient_name = get_patient_first_name(session['patient_fio'])
    
    message = f"""Уважаемый {patient_name}, напоминаем вам, что через 5 минут начнётся телемедицинская консультация.

Пожалуйста, пройдите по ссылке в чат:
{session['chat_invite_link']}"""
    
    return message


def build_reminder_15m_without_consent(session: Dict[str, Any]) -> str:
    """
    Напоминание за 5 минут без ссылки (согласия нет)
    
    Args:
        session: Данные сессии из БД
        
    Returns:
        Текст сообщения
    """
    patient_name = get_patient_first_name(session['patient_fio'])
    
    message = f"""Уважаемый {patient_name}, напоминаем вам, что через 5 минут начнётся телемедицинская консультация."""
    
    return message


def build_late_consent_message(session: Dict[str, Any]) -> str:
    """
    Сообщение при позднем согласии (после 5-минутного напоминания)
    
    Args:
        session: Данные сессии из БД
        
    Returns:
        Текст сообщения
    """
    patient_name = get_patient_first_name(session['patient_fio'])
    
    message = f"""Уважаемый {patient_name}, вы дали согласие на телемедицинскую консультацию.

Пожалуйста, пройдите по ссылке в чат с врачом:
{session['chat_invite_link']}"""
    
    return message


def build_consent_confirmation_message() -> str:
    """
    Сообщение-подтверждение после нажатия кнопки "Согласен"
    
    Returns:
        Текст сообщения
    """
    message = """Ваше согласие принято.

Если вы передумали, отменить запись можно по телефону 122"""
    
    return message


def build_consent_after_consultation_message() -> str:
    """
    Сообщение при нажатии "Согласен" после времени консультации.
    
    Returns:
        Текст сообщения
    """
    message = """Ваше согласие зафиксировано.

Обратите внимание: время консультации уже прошло. При необходимости запишитесь на приём заново.
Отменить или уточнить запись можно по телефону 122."""
    
    return message


def build_cancellation_message(session: Dict[str, Any]) -> str:
    """
    Сообщение об отмене консультации
    
    Args:
        session: Данные сессии из БД
        
    Returns:
        Текст сообщения
    """
    patient_name = get_patient_first_name(session['patient_fio'])
    date_str, time_str = format_datetime_russian(session['schedule_date'])
    
    message = f"""Уважаемый {patient_name}, телемедконсультация отменена.

МО: {session['clinic_name']}
Врач: {session['doctor_fio']}
Дата: {date_str}
Время: {time_str} (Московское время)

Уточнить причину отмены можно в регистратуре мед организации."""
    
    return message
