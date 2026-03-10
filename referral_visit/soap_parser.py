# referral_visit/soap_parser.py
"""Парсинг ответов МИС для сценария записи по направлению."""
import re
from typing import List, Dict, Any, Optional

from visit_a_doctor.soap_parser import SoapResponseParser


def _clean_xml(xml_content: str) -> str:
    xml_clean = re.sub(r'\s+xmlns:?[^=]*="[^"]+"', '', xml_content)
    xml_clean = re.sub(r'<(\w+):', '<', xml_clean)
    xml_clean = re.sub(r'</(\w+):', '</', xml_clean)
    return xml_clean


def _text(el, tag: str, default: str = "") -> str:
    if el is None:
        return default
    child = el.find(tag)
    return (child.text or "").strip() if child is not None else default


def parse_referrals(xml_content: str) -> List[Dict[str, Any]]:
    """
    Парсит список направлений из GetPatientInfoResponse (Pass_referral=1) или из GetReferralInfoResponse.
    Возвращает только направления с Referral_Type in (4,6,8) и Available_Record = AVAILABLE.
    """
    result = []
    try:
        xml_clean = _clean_xml(xml_content)
        import xml.etree.ElementTree as ET
        root = ET.fromstring(xml_clean)

        for ref_el in root.findall(".//Referral"):
            ref_type_el = ref_el.find("Referral_Type")
            ref_type = int(ref_type_el.text) if ref_type_el is not None and ref_type_el.text else 0
            if ref_type not in (4, 6, 8):
                continue

            avail = _text(ref_el, "Available_Record", "")
            if avail != "AVAILABLE":
                continue

            ref_id = _text(ref_el, "Referral_Id")
            ref_number = _text(ref_el, "Referral_Number")
            start_date = _text(ref_el, "Referral_Start_Date")
            end_date = _text(ref_el, "Referral_End_Date")
            to_mo_oid_el = ref_el.find("To_MO_OID")
            to_mo_oid = (to_mo_oid_el.text or "").strip() if to_mo_oid_el is not None else ""
            if not to_mo_oid and ref_el.findall("To_MO_OID"):
                to_mo_oid = (ref_el.findall("To_MO_OID")[0].text or "").strip()

            post_id = ""
            to_post = ref_el.find("To_Service_Post")
            if to_post is not None:
                post_el = to_post.find("Post")
                if post_el is not None:
                    pid_el = post_el.find("Post_Id")
                    if pid_el is not None and pid_el.text:
                        post_id = pid_el.text.strip()

            specialty_id = ""
            service_id = ""
            to_spec = ref_el.find("To_Service_Specialty")
            if to_spec is not None:
                sid_el = to_spec.find("Specialty_Id")
                if sid_el is not None and sid_el.text:
                    specialty_id = sid_el.text.strip()
                svc_info = to_spec.find("Services_Info")
                if svc_info is not None:
                    svc_el = svc_info.find("Service")
                    if svc_el is not None:
                        s_el = svc_el.find("Service_Id")
                        if s_el is not None and s_el.text:
                            service_id = s_el.text.strip()

            to_resource_name = _text(ref_el, "To_Resource_Name")
            to_resource_snils = _text(ref_el, "To_Resource_Snils")
            from_resource_name = _text(ref_el, "From_Resource_Name")
            reason_not = _text(ref_el, "Reason_Not_Available")

            result.append({
                "referral_id": ref_id,
                "referral_number": ref_number,
                "referral_type": ref_type,
                "referral_start_date": start_date,
                "referral_end_date": end_date,
                "to_mo_oid": to_mo_oid,
                "post_id": post_id,
                "specialty_id": specialty_id,
                "service_id": service_id,
                "to_resource_name": to_resource_name,
                "to_resource_snils": to_resource_snils,
                "from_resource_name": from_resource_name,
                "available_record": avail,
                "reason_not_available": reason_not,
            })
    except Exception as e:
        print(f"referral_visit parse_referrals error: {e}")
    return result


def parse_get_referral_info_response(xml_content: str) -> Dict[str, Any]:
    """
    Парсит ответ GetReferralInfoResponse.
    Возвращает dict: {"error_code": str|None, "referrals": list, "session_id": str}.
    Если ошибка — error_code заполнен, referrals пустой. Иначе — referrals список, error_code None.
    """
    out = {"error_code": None, "referrals": [], "session_id": None}
    try:
        session_id = SoapResponseParser.parse_session_id(xml_content)
        out["session_id"] = session_id

        err_el = re.search(r"<Error_Code>([^<]+)</Error_Code>", xml_content)
        if err_el:
            out["error_code"] = err_el.group(1).strip()
            return out

        out["referrals"] = parse_referrals(xml_content)
    except Exception as e:
        print(f"parse_get_referral_info_response error: {e}")
        out["error_code"] = "UNDEFINED_ERROR"
    return out
