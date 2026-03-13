"""Состояние пользователя для сценария записи по направлению ДРУГОГО человека."""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional


@dataclass
class ReferralOtherUserContext:
    user_id: int
    # Шаги: OTHER_PERSON_SELECT, OTHER_ENTER_FIO, OTHER_ENTER_BIRTHDATE,
    # OTHER_ENTER_OMS, OTHER_CONFIRM_PATIENT, OTHER_ENTER_REF_NUMBER,
    # OTHER_REF_LIST, OTHER_DOCTOR, OTHER_DATE, OTHER_TIME, OTHER_CONFIRM_BOOKING
    step: str = "OTHER_INIT"

    created_at: datetime = field(default_factory=datetime.now)
    last_activity: datetime = field(default_factory=datetime.now)
    session_created_at: Optional[datetime] = None

    # SOAP-сессия (как в visit_a_doctor)
    session_id: str = ""
    client_session_id: str = ""

    # Данные пациента, которого записываем
    selected_person: str = "other"
    patient_fio: str = ""
    patient_birthdate: str = ""
    patient_gender: str = ""
    patient_snils: str = ""
    patient_oms: str = ""
    is_from_rms: bool = False
    return_to_confirm: bool = False

    # Кандидаты из РМИС (по телефону владельца)
    family_candidates: List[Dict[str, Any]] = field(default_factory=list)

    # Данные направления
    referral_number: str = ""
    referrals: List[Dict[str, Any]] = field(default_factory=list)
    selected_referral_id: str = ""
    selected_referral: Optional[Dict[str, Any]] = None
    selected_referral_number: str = ""
    referral_start_date: str = ""
    referral_end_date: str = ""

    # МО и подразделения
    selected_mo_id: str = ""
    selected_mo_oid: str = ""
    selected_mo_name: str = ""
    selected_post_id: str = ""
    selected_spec: str = ""
    selected_specialty_id: str = ""
    selected_service_id: str = ""

    # Врач / кабинет
    selected_doctor_id: str = ""
    selected_doctor_name: str = ""
    selected_resource_type: str = "specialist"
    selected_room_id: str = ""
    selected_room_oid: str = ""
    selected_room: str = ""

    # Слот
    selected_date: str = ""
    selected_time: str = ""
    selected_slot_id: str = ""

    # Кэши
    available_dates_cache: Optional[List[str]] = None
    available_slots_cache: Optional[List[Dict[str, Any]]] = None
    date_page: int = 0
    time_page: int = 0
    ref_list_page: int = 0

    def update_activity(self) -> None:
        self.last_activity = datetime.now()

    def is_expired(self, timeout_minutes: int = 30) -> bool:
        if self.last_activity is None:
            return True
        return (datetime.now() - self.last_activity) > timedelta(minutes=timeout_minutes)

    def is_session_expired(self, session_timeout_minutes: int = 60) -> bool:
        if not self.session_id:
            return True
        if self.session_created_at is None:
            return False
        return (datetime.now() - self.session_created_at) > timedelta(minutes=session_timeout_minutes)

