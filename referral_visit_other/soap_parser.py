"""
Парсинг ответов МИС для сценария записи по направлению ДРУГОГО человека.

Фактически переиспользует логику из referral_visit.soap_parser.
"""
from typing import List, Dict, Any, Optional

from referral_visit.soap_parser import (
    parse_session_id,
    parse_referrals,
    parse_get_referral_info_response,
    parse_mo_list,
    parse_doctors,
    parse_slots,
    parse_create_appointment_details,
)

__all__ = [
    "parse_session_id",
    "parse_referrals",
    "parse_get_referral_info_response",
    "parse_mo_list",
    "parse_doctors",
    "parse_slots",
    "parse_create_appointment_details",
]

