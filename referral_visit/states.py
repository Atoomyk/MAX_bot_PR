# referral_visit/states.py
"""Состояние пользователя для сценария записи по направлению."""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional


@dataclass
class ReferralUserContext:
    user_id: int
    step: str = "REF_INIT"  # REF_PERSON, REF_PATIENT_CONFIRM, REF_LIST, REF_ENTER_NUMBER, REF_DOCTOR, REF_DATE, REF_TIME, REF_CONFIRM

    created_at: datetime = field(default_factory=datetime.now)
    last_activity: datetime = field(default_factory=datetime.now)
    session_created_at: Optional[datetime] = None

    session_id: str = ""
    client_session_id: str = ""

    selected_person: str = ""
    patient_fio: str = ""
    patient_birthdate: str = ""
    patient_gender: str = ""
    patient_snils: str = ""
    patient_oms: str = ""
    return_to_confirm: bool = False
    is_from_rms: bool = False

    referrals: List[Dict[str, Any]] = field(default_factory=list)
    selected_referral_id: str = ""
    selected_referral: Optional[Dict[str, Any]] = None
    selected_referral_number: str = ""
    referral_start_date: str = ""
    referral_end_date: str = ""

    selected_mo_id: str = ""
    selected_mo_oid: str = ""
    selected_mo_name: str = ""
    selected_post_id: str = ""
    selected_spec: str = ""
    selected_specialty_id: str = ""
    selected_service_id: str = ""

    selected_doctor_id: str = ""
    selected_doctor_name: str = ""
    selected_resource_type: str = "specialist"
    selected_room_id: str = ""
    selected_room_oid: str = ""
    selected_room: str = ""

    selected_date: str = ""
    selected_time: str = ""
    selected_slot_id: str = ""

    available_dates_cache: Optional[List[str]] = None
    available_slots_cache: Optional[List[Dict]] = None
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
