"""
Парсинг ответов МИС для сценария записи по направлению.

Отдельный парсер, не зависящий от visit_a_doctor.
"""
import re
from typing import List, Dict, Any, Optional
import xml.etree.ElementTree as ET


def _clean_xml(xml_content: str) -> str:
    xml_clean = re.sub(r'\s+xmlns:?[^=]*="[^"]+"', "", xml_content)
    xml_clean = re.sub(r"<(\w+):", "<", xml_clean)
    xml_clean = re.sub(r"</(\w+):", "</", xml_clean)
    return xml_clean


def _text(el, tag: str, default: str = "") -> str:
    if el is None:
        return default
    child = el.find(tag)
    return (child.text or "").strip() if child is not None else default


def parse_session_id(xml_content: str) -> Optional[str]:
    """Парсит Session_ID из любого ответа с учётом неймспейсов."""
    try:
        if "Session_ID" in xml_content:
            m = re.search(r"<Session_ID>([^<]+)</Session_ID>", xml_content)
            if m:
                return m.group(1).strip()
        xml_clean = _clean_xml(xml_content)
        root = ET.fromstring(xml_clean)
        for node in root.iter():
            if node.tag == "Session_ID" and node.text:
                return node.text.strip()
    except Exception as e:
        print(f"referral_visit parse_session_id error: {e}")
    return None


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
        session_id = parse_session_id(xml_content)
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


def parse_mo_list(xml_content: str) -> List[Dict[str, str]]:
    """Парсит список МО (GetMOInfoExtendedResponse) для направлений."""
    mos: List[Dict[str, str]] = []
    try:
        xml_clean = _clean_xml(xml_content)
        root = ET.fromstring(xml_clean)
        for mo_node in root.findall(".//MO"):
            mo_id = mo_node.find("MO_Id")
            name = mo_node.find("MO_Name")
            oid = mo_node.find("MO_OID")
            address = mo_node.find("MO_Address")
            if mo_id is not None and name is not None:
                mos.append(
                    {
                        "id": mo_id.text,
                        "name": name.text,
                        "oid": oid.text if oid is not None else "",
                        "address": address.text if address is not None else "",
                    }
                )
    except Exception as e:
        print(f"referral_visit parse_mo_list error: {e}")
    return mos


def parse_doctors(xml_content: str) -> List[Dict[str, str]]:
    """
    Парсит список ресурсов (GetMOResourceInfoResponse) для направлений.
    Возвращает элементы с полями: id, name, dates, type, mo_oid, room_id/room_oid (для кабинета).
    """
    doctors: List[Dict[str, Any]] = []
    try:
        xml_clean = _clean_xml(xml_content)
        root = ET.fromstring(xml_clean)

        for mo_available in root.findall(".//MO_Available"):
            mo_node = mo_available.find("MO")
            mo_oid_el = mo_node.find("MO_OID") if mo_node is not None else None
            mo_oid_text = (mo_oid_el.text or "").strip() if mo_oid_el is not None else ""

            for resource in mo_available.findall(".//Resource"):
                specialist = resource.find("Specialist")
                room = resource.find("Room")

                dates: List[str] = []
                avail_dates = resource.find("Available_Dates")
                if avail_dates is not None:
                    for d in avail_dates.findall("Available_Date"):
                        raw_date = (d.text or "")[:10]
                        if not raw_date:
                            continue
                        formatted_date = f"{raw_date[8:10]}.{raw_date[5:7]}.{raw_date[0:4]}"
                        dates.append(formatted_date)

                if not dates:
                    continue

                if specialist is not None:
                    last = specialist.find("Last_Name")
                    first = specialist.find("First_Name")
                    middle = specialist.find("Middle_Name")
                    snils = specialist.find("SNILS")

                    if last is not None and first is not None and middle is not None and snils is not None:
                        last_name = last.text or ""
                        first_name = first.text or ""
                        middle_name = middle.text or ""
                        snils_text = snils.text or ""

                        full_name = f"{last_name} {first_name[0]}.{middle_name[0]}."

                        doctors.append(
                            {
                                "id": snils_text,
                                "name": full_name,
                                "dates": dates,
                                "type": "specialist",
                                "mo_oid": mo_oid_text,
                            }
                        )

                elif room is not None:
                    room_id = room.find("Room_Id")
                    room_number = room.find("Room_Number")
                    room_name = room.find("Room_Name")
                    room_oid = room.find("Room_OID")

                    if room_id is not None:
                        room_id_text = room_id.text or ""
                        room_number_text = room_number.text if room_number is not None else ""
                        room_name_text = room_name.text if room_name is not None else ""
                        room_oid_text = room_oid.text if room_oid is not None else ""

                        display_name = (
                            room_name_text
                            if room_name_text
                            else f"Кабинет {room_number_text}" if room_number_text else f"Кабинет {room_id_text}"
                        )

                        doctors.append(
                            {
                                "id": f"ROOM_{room_id_text}",
                                "name": display_name,
                                "dates": dates,
                                "type": "room",
                                "room_id": room_id_text,
                                "room_oid": room_oid_text,
                                "mo_oid": mo_oid_text,
                            }
                        )
    except Exception as e:
        print(f"referral_visit parse_doctors error: {e}")
    return doctors


def parse_slots(xml_content: str) -> List[Dict[str, str]]:
    """Парсит слоты (GetScheduleInfoResponse) для направлений."""
    slots: List[Dict[str, str]] = []
    try:
        xml_clean = _clean_xml(xml_content)
        root = ET.fromstring(xml_clean)

        for slot in root.findall(".//Slots"):
            slot_id_el = slot.find("Slot_Id")
            visit_time_el = slot.find("VisitTime")
            room_el = slot.find("Room")
            if slot_id_el is None or visit_time_el is None:
                continue
            slot_id = slot_id_el.text
            visit_time = visit_time_el.text
            room = room_el.text if room_el is not None else ""
            if not visit_time:
                continue
            time_str = visit_time[11:16]
            slots.append({"id": slot_id, "time": time_str, "room": room})
    except Exception as e:
        print(f"referral_visit parse_slots error: {e}")
    return slots


def parse_create_appointment_details(xml_content: str) -> Dict[str, Any]:
    """
    Парсит CreateAppointmentResponse и возвращает ключевые поля
    для сценария направлений.
    """
    def _val(tag: str) -> Optional[str]:
        m = re.search(rf"<{tag}>([^<]+)</{tag}>", xml_content)
        return m.group(1).strip() if m else None

    return {
        "status_code": _val("Status_Code"),
        "comment": _val("Comment"),
        "book_id_mis": _val("Book_Id_Mis"),
        "visit_time": _val("Visit_Time"),
        "room": _val("Room"),
        "slot_id": _val("Slot_Id"),
        "session_id": _val("Session_ID"),
    }
