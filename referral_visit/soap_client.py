"""
SOAP-запросы для сценария записи по направлению.

Отдельный клиент, не зависящий от visit_a_doctor.
"""
import os
import aiohttp
from dotenv import load_dotenv

from logging_config import log_data_event
from phone_utils import normalize_phone_plus7

load_dotenv()

SOAP_URL = os.getenv("SOAP_URL")
SOAP_HEADERS = {"Content-Type": "text/xml; charset=utf-8"}

# Параметр для включения/отключения логирования SOAP-запросов/ответов по направлению.
# 1 / true / yes (в любом регистре) — включено, иначе выключено.
_LOG_REFERRAL_SOAP = os.getenv("REFERRAL_SOAP_LOGGING", "1").lower() in ("1", "true", "yes")


async def _send_request(xml_body: str, soap_action: str, user_id: int | None = None) -> str:
    """
    Общая отправка SOAP-запросов для сценария записи по направлению.
    Логирует исходящий XML и ответ для диагностики.
    """
    uid = user_id if user_id is not None else 0

    if _LOG_REFERRAL_SOAP:
        try:
            log_data_event(uid, "referral_soap_request", soap_action=soap_action, xml=xml_body)
        except Exception:
            # Логирование не должно ломать основной поток
            pass

    headers = SOAP_HEADERS.copy()
    headers["SOAPAction"] = soap_action

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(SOAP_URL, data=xml_body.encode("utf-8"), headers=headers, timeout=10) as response:
                response_text = await response.text()
    except Exception as e:
        error_msg = str(e)
        if _LOG_REFERRAL_SOAP:
            try:
                log_data_event(uid, "referral_soap_error", soap_action=soap_action, error=error_msg)
            except Exception:
                pass
        # Пробрасываем исключение для обработки в handlers
        raise Exception(f"SOAP connection error: {error_msg}")

    if _LOG_REFERRAL_SOAP:
        try:
            log_data_event(uid, "referral_soap_response", soap_action=soap_action, xml=response_text)
        except Exception:
            pass

    return response_text


async def get_patient_referrals(
    snils: str,
    oms: str,
    birthdate: str,
    fio_parts: list,
    gender: str,
    client_session_id: str,
    phone: str,
) -> str:
    """GetPatientInfo с Pass_referral=1 — возвращает направления гражданина."""
    sex_val = "M" if gender == "Мужской" else "F"
    phone_norm = normalize_phone_plus7(phone)
    xml = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <GetPatientInfoRequest xmlns="http://www.rt-eu.ru/med/er/v2_0">
            <Session_ID>{client_session_id}</Session_ID>
            <Patient_Data>
                <OMS_Number>{oms}</OMS_Number>
                <SNILS>{snils}</SNILS>
                <First_Name>{fio_parts[1]}</First_Name>
                <Last_Name>{fio_parts[0]}</Last_Name>
                <Middle_Name>{fio_parts[2]}</Middle_Name>
                <Birth_Date>{birthdate}</Birth_Date>
                <Phone>{phone_norm}</Phone>
                <Sex>{sex_val}</Sex>
            </Patient_Data>
            <Pass_referral>1</Pass_referral>
        </GetPatientInfoRequest>
    </soapenv:Body>
</soapenv:Envelope>"""
    return await _send_request(xml, "GetPatientInfo")


async def get_referral_by_number(
    session_id: str,
    snils: str,
    oms: str,
    birthdate: str,
    fio_parts: list,
    gender: str,
    referral_number: str,
) -> str:
    """GetReferralInfo — информация о направлении по номеру."""
    sex_val = "M" if gender == "Мужской" else "F"
    xml = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <GetReferralInfoRequest xmlns="http://www.rt-eu.ru/med/er/v2_0">
            <Session_ID>{session_id}</Session_ID>
            <Patient_Data>
                <OMS_Number>{oms}</OMS_Number>
                <SNILS>{snils}</SNILS>
                <First_Name>{fio_parts[1]}</First_Name>
                <Last_Name>{fio_parts[0]}</Last_Name>
                <Middle_Name>{fio_parts[2]}</Middle_Name>
                <Birth_Date>{birthdate}</Birth_Date>
                <Sex>{sex_val}</Sex>
            </Patient_Data>
            <Referral_Number>{referral_number}</Referral_Number>
        </GetReferralInfoRequest>
    </soapenv:Body>
</soapenv:Envelope>"""
    return await _send_request(xml, "GetReferralInfo")


async def get_doctors_referral(
    session_id: str,
    post_id: str,
    mo_oid: str,
    referral_id: str,
    start_date: str,
    end_date: str,
) -> str:
    """GetMOResourceInfo с диапазоном дат из направления. start_date/end_date в формате YYYY-MM-DD."""
    xml = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <GetMOResourceInfoRequest xmlns="http://www.rt-eu.ru/med/er/v2_0">
            <Session_ID>{session_id}</Session_ID>
            <Service_Posts>
                <Post>
                    <Post_Id>{post_id}</Post_Id>
                </Post>
            </Service_Posts>
            <MO_OID_List>
                <MO_OID>{mo_oid}</MO_OID>
            </MO_OID_List>
            <Referral_Id>{referral_id}</Referral_Id>
            <Start_Date_Range>{start_date}</Start_Date_Range>
            <End_Date_Range>{end_date}</End_Date_Range>
        </GetMOResourceInfoRequest>
    </soapenv:Body>
</soapenv:Envelope>"""
    return await _send_request(xml, "GetMOResourceInfo")


async def get_slots_referral(
    session_id: str,
    specialist_snils: str,
    mo_oid: str,
    post_id: str,
    referral_id: str,
    date: str,
    room_id: str | None = None,
    room_oid: str | None = None,
) -> str:
    """
    GetScheduleInfo для сценария направлений.
    date в формате DD.MM.YYYY (как из бота).
    """
    from datetime import datetime as _dt

    d_obj = _dt.strptime(date, "%d.%m.%Y")
    fmt_date = d_obj.strftime("%Y-%m-%d")

    is_room_mode = (room_id or room_oid) and not specialist_snils

    if is_room_mode:
        room_oid_part = f"\n            <Room_OID>{room_oid}</Room_OID>" if room_oid else ""
        room_id_part = f"\n            <Room_Id>{room_id}</Room_Id>" if room_id else ""

        xml = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <GetScheduleInfoRequest xmlns="http://www.rt-eu.ru/med/er/v2_0">
            <Session_ID>{session_id}</Session_ID>{room_oid_part}{room_id_part}
            <MO_OID>{mo_oid}</MO_OID>
            <Service_Post>
                <Post>
                    <Post_Id>{post_id}</Post_Id>
                </Post>
            </Service_Post>
            <Referral_Id>{referral_id}</Referral_Id>
            <Start_Date_Range>{fmt_date}</Start_Date_Range>
            <End_Date_Range>{fmt_date}</End_Date_Range>
            <Start_Time_Range>00:00:00</Start_Time_Range>
            <End_Time_Range>23:59:00</End_Time_Range>
        </GetScheduleInfoRequest>
    </soapenv:Body>
</soapenv:Envelope>"""
    else:
        specialist_snils_value = specialist_snils if specialist_snils else ""

        xml = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <GetScheduleInfoRequest xmlns="http://www.rt-eu.ru/med/er/v2_0">
            <Session_ID>{session_id}</Session_ID>
            <Specialist_SNILS>{specialist_snils_value}</Specialist_SNILS>
            <MO_OID>{mo_oid}</MO_OID>
            <Service_Post>
                <Post>
                    <Post_Id>{post_id}</Post_Id>
                </Post>
            </Service_Post>
            <Referral_Id>{referral_id}</Referral_Id>
            <Start_Date_Range>{fmt_date}</Start_Date_Range>
            <End_Date_Range>{fmt_date}</End_Date_Range>
            <Start_Time_Range>00:00:00</Start_Time_Range>
            <End_Time_Range>23:59:00</End_Time_Range>
        </GetScheduleInfoRequest>
    </soapenv:Body>
</soapenv:Envelope>"""

    return await _send_request(xml, "GetScheduleInfo")


async def book_appointment_referral(session_id: str, slot_id: str) -> str:
    """CreateAppointment для сценария направлений."""
    xml = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <CreateAppointmentRequest xmlns="http://www.rt-eu.ru/med/er/v2_0">
            <Session_ID>{session_id}</Session_ID>
            <Slot_Id>{slot_id}</Slot_Id>
        </CreateAppointmentRequest>
    </soapenv:Body>
</soapenv:Envelope>"""
    return await _send_request(xml, "CreateAppointment")
