# visit_a_doctor/states.py
"""
Управление состоянием пользователя для сценария записи к врачу
"""
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timedelta

@dataclass
class UserContext:
    user_id: int
    step: str = "INIT"  # INIT, PERSON, MO, SPEC, DOCTOR, DATE, TIME, CONFIRM
    return_to_confirm: bool = False # Флаг режима редактирования
    
    # Временные метки для контроля таймаутов
    created_at: datetime = field(default_factory=datetime.now)
    last_activity: datetime = field(default_factory=datetime.now)
    session_created_at: datetime = field(default=None)  # Время создания SOAP-сессии
    
    # Данные выбора
    selected_person: str = "" # "me" or "other"
    selected_mo_id: str = ""
    selected_spec: str = ""
    selected_doctor_id: str = ""
    selected_doctor_name: str = "" # Сохраняем ФИО врача
    selected_date: str = ""
    selected_time: str = ""
    
    # SOAP Context
    session_id: str = ""
    selected_mo_oid: str = ""
    selected_post_id: str = ""
    selected_slot_id: str = "" # ID слота для записи
    selected_room: str = "" # Кабинет
    available_dates_cache: list = None # Кэш дат для врача
    available_slots_cache: list = None # Кэш слотов (id, time)
    
    # Данные пациента (если selected_person == "other")
    patient_fio: str = ""
    patient_birthdate: str = ""
    patient_gender: str = ""
    patient_snils: str = ""
    patient_oms: str = ""
    
    # Пагинация
    spec_page: int = 0
    date_page: int = 0
    time_page: int = 0
    
    def update_activity(self):
        """Обновляет время последней активности"""
        self.last_activity = datetime.now()
    
    def is_expired(self, timeout_minutes: int = 30) -> bool:
        """
        Проверяет, истекло ли время неактивности
        
        Args:
            timeout_minutes: Таймаут в минутах (по умолчанию 30)
        
        Returns:
            True если время истекло, False иначе
        """
        if self.last_activity is None:
            return True
        elapsed = datetime.now() - self.last_activity
        return elapsed > timedelta(minutes=timeout_minutes)
    
    def is_session_expired(self, session_timeout_minutes: int = 60) -> bool:
        """
        Проверяет, истекла ли SOAP-сессия
        
        Args:
            session_timeout_minutes: Таймаут сессии в минутах (по умолчанию 60)
        
        Returns:
            True если сессия истекла или отсутствует, False иначе
        """
        if not self.session_id:
            return True
        if self.session_created_at is None:
            return False  # Если время создания не установлено, считаем валидной
        elapsed = datetime.now() - self.session_created_at
        return elapsed > timedelta(minutes=session_timeout_minutes)
