# visit_a_doctor/soap_client.py
"""
Клиент для отправки SOAP-запросов.
"""
import aiohttp
import os
from datetime import datetime, timedelta

from dotenv import load_dotenv

load_dotenv()

# Конфигурация (в реальном проекте загружать из .env)
# Используем URL из переменной окружения, или дефолтный (тестовый) если не задан
SOAP_URL = os.getenv("SOAP_URL")
SOAP_HEADERS = {
    "Content-Type": "text/xml; charset=utf-8",
}

class SoapClient:

    @staticmethod
    async def _send_request(xml_body: str, soap_action: str) -> str:
        """Отправляет POST запрос с XML"""
        headers = SOAP_HEADERS.copy()
        headers["SOAPAction"] = soap_action

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(SOAP_URL, data=xml_body.encode('utf-8'), headers=headers, timeout=10) as response:
                    return await response.text()

            except Exception as e:
                error_msg = str(e)
                print(f"SOAP Connection Error: {e}")
                # Пробрасываем исключение для обработки в handlers
                raise Exception(f"SOAP connection error: {error_msg}")

    @staticmethod
    async def get_patient_session(snils, oms, birthdate, fio_parts, gender, client_session_id) -> str:
        """
        1. GetPatientInfoRequest
        Возвращает XML для парсинга
        """
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
            <Pass_referral>0</Pass_referral>
        </GetPatientInfoRequest>
    </soapenv:Body>
</soapenv:Envelope>"""
        return await SoapClient._send_request(xml, "GetPatientInfo")

    @staticmethod
    async def get_mos(session_id: str) -> str:
        """3. GetMOInfoExtendedRequest"""
        xml = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <GetMOInfoExtendedRequest xmlns="http://www.rt-eu.ru/med/er/v2_0">
            <Session_ID>{session_id}</Session_ID>
            <Booking_Type>APPOINTMENT</Booking_Type>
        </GetMOInfoExtendedRequest>
    </soapenv:Body>
</soapenv:Envelope>"""
        return await SoapClient._send_request(xml, "GetMOInfoExtended")

    @staticmethod
    async def get_specs(session_id: str, mo_id: str) -> str:
        """5. GetServicePostSpecsInfoRequest"""
        xml = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <GetServicePostSpecsInfoRequest xmlns="http://www.rt-eu.ru/med/er/v2_0">
            <Session_ID>{session_id}</Session_ID>
            <MO_Id>{mo_id}</MO_Id>
        </GetServicePostSpecsInfoRequest>
    </soapenv:Body>
</soapenv:Envelope>"""
        return await SoapClient._send_request(xml, "GetServicePostSpecsInfo")

    @staticmethod
    async def get_doctors(session_id: str, post_id: str, mo_oid: str) -> str:
        """7. GetMOResourceInfoRequest"""
        # Дата: следующие 14 дней
        start_date = datetime.now().strftime("%Y-%m-%d")
        end_date = (datetime.now() + timedelta(days=14)).strftime("%Y-%m-%d")
        
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
        return await SoapClient._send_request(xml, "GetMOResourceInfo")

    @staticmethod
    async def get_slots(session_id: str, specialist_snils: str, mo_oid: str, post_id: str, date: str, room_id: str = None) -> str:
        """9. GetScheduleInfoRequest"""
        # date expected format YYYY-MM-DD for Request? The example shows ranges. 
        # But we are selecting a specific date. Let's use that date as Range.
        # Input date from bot is DD.MM.YYYY
        d_obj = datetime.strptime(date, "%d.%m.%Y")
        fmt_date = d_obj.strftime("%Y-%m-%d")
        
        # Для кабинетов specialist_snils может быть пустым
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
            <Start_Date_Range>{fmt_date}</Start_Date_Range>
            <End_Date_Range>{fmt_date}</End_Date_Range>
            <Start_Time_Range>00:00:00</Start_Time_Range>
            <End_Time_Range>23:59:00</End_Time_Range>
        </GetScheduleInfoRequest>
    </soapenv:Body>
</soapenv:Envelope>"""
        return await SoapClient._send_request(xml, "GetScheduleInfo")

    @staticmethod
    async def book_appointment(session_id: str, slot_id: str) -> str:
        """11. CreateAppointmentRequest"""
        xml = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <CreateAppointmentRequest xmlns="http://www.rt-eu.ru/med/er/v2_0">
            <Session_ID>{session_id}</Session_ID>
            <Slot_Id>{slot_id}</Slot_Id>
        </CreateAppointmentRequest>
    </soapenv:Body>
</soapenv:Envelope>"""
        return await SoapClient._send_request(xml, "CreateAppointment")
