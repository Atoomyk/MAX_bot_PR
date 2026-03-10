# referral_visit/soap_client.py
"""SOAP-запросы для записи по направлению (GetPatientInfo Pass_referral=1, GetReferralInfo, GetMOResourceInfo с датами направления)."""
import os
from dotenv import load_dotenv
load_dotenv()

SOAP_URL = os.getenv("SOAP_URL")
SOAP_HEADERS = {"Content-Type": "text/xml; charset=utf-8"}

# Переиспользуем _send_request из visit_a_doctor
from visit_a_doctor.soap_client import SoapClient as VisitSoapClient


async def _send_request(xml_body: str, soap_action: str) -> str:
    return await VisitSoapClient._send_request(xml_body, soap_action)


async def get_patient_referrals(
    snils: str,
    oms: str,
    birthdate: str,
    fio_parts: list,
    gender: str,
    client_session_id: str,
) -> str:
    """GetPatientInfo с Pass_referral=1 — возвращает направления гражданина."""
    sex_val = "M" if gender == "Мужской" else "F"
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
            <Start_Date_Range>{start_date}</Start_Date_Range>
            <End_Date_Range>{end_date}</End_Date_Range>
        </GetMOResourceInfoRequest>
    </soapenv:Body>
</soapenv:Envelope>"""
    return await _send_request(xml, "GetMOResourceInfo")
