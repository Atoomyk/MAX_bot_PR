import os
import uuid
import aiohttp
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

from dotenv import load_dotenv
from maxapi import Bot

from bot_utils import create_keyboard
from logging_config import log_system_event, log_user_event
from patient_api_client import get_patients_by_phone
from sync_appointments.cancel_service import CancelService
from user_database import db
from visit_a_doctor.soap_parser import SoapResponseParser

load_dotenv()

SOAP_URL = os.getenv("SOAP_URL")
SOAP_URL_PATIENT_ID = os.getenv("SOAP_URL_PatientID")
MY_APPOINTMENTS_LOGGING = os.getenv("MY_APPOINTMENTS_LOGGING", "0")
PAGE_SIZE = 5

_sessions: Dict[int, Dict[str, Any]] = {}


def _norm(s: Any) -> str:
    return str(s or "").strip()


def _is_myapps_logging_enabled() -> bool:
    return _norm(MY_APPOINTMENTS_LOGGING) == "1"


def _log_mis_io(event: str, **kwargs: Any) -> None:
    if not _is_myapps_logging_enabled():
        return
    try:
        log_system_event("my_appointments", event, **kwargs)
    except Exception:
        # Логирование не должно ломать основной сценарий
        pass


def _normalize_birth_date(raw: str) -> str:
    raw = _norm(raw)
    if not raw:
        return ""
    if "." in raw:
        parts = raw.split(".")
        if len(parts) == 3:
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
    if len(raw) >= 10:
        return raw[:10]
    return raw


def _gender_to_soap(gender: str) -> str:
    g = _norm(gender).lower()
    if g in ("мужской", "m", "male", "м"):
        return "M"
    return "F"


def _parse_visit_time(raw: str) -> Optional[datetime]:
    s = _norm(raw)
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _is_same_patient(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    a_snils = "".join(ch for ch in _norm(a.get("snils")) if ch.isdigit())
    b_snils = "".join(ch for ch in _norm(b.get("snils")) if ch.isdigit())
    if a_snils and b_snils and a_snils == b_snils:
        return True
    return (
        _norm(a.get("fio")).lower() == _norm(b.get("fio")).lower()
        and _normalize_birth_date(_norm(a.get("birth_date"))) == _normalize_birth_date(_norm(b.get("birth_date")))
    )


def _extract_local_patients(user_id: int) -> List[Dict[str, Any]]:
    try:
        db.cursor.execute(
            """
            SELECT appointment_json
            FROM appointments
            WHERE user_id = %s
              AND status = 'active'
              AND booking_source = 'other_bot'
            """,
            (user_id,),
        )
        rows = db.cursor.fetchall() or []
    except Exception as e:
        log_system_event("my_appointments", "local_patients_read_failed", error=str(e), user_id=user_id)
        return []

    patients: List[Dict[str, Any]] = []
    for row in rows:
        try:
            data = row[0] or {}
            original = data.get("Исходные_данные") or {}
            patient = {
                "fio": _norm(original.get("ФИО пациента")),
                "birth_date": _norm(original.get("Дата рождения")),
                "snils": _norm(original.get("СНИЛС")),
                "oms": _norm(original.get("ОМС")),
                "gender": _norm(original.get("Пол")),
                "source": "local",
            }
            if not patient["fio"]:
                continue
            if not any(_is_same_patient(patient, p) for p in patients):
                patients.append(patient)
        except Exception:
            continue
    return patients


def _extract_local_appointments(user_id: int, patient: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        db.cursor.execute(
            """
            SELECT appointment_json, external_visit_time
            FROM appointments
            WHERE user_id = %s
              AND status = 'active'
            ORDER BY external_visit_time ASC NULLS LAST
            """,
            (user_id,),
        )
        rows = db.cursor.fetchall() or []
    except Exception as e:
        log_system_event("my_appointments", "local_apps_read_failed", error=str(e), user_id=user_id)
        return []

    now = datetime.now().astimezone()
    result: List[Dict[str, Any]] = []
    for row in rows:
        data = row[0] or {}
        original = data.get("Исходные_данные") or {}
        cand = {
            "fio": _norm(original.get("ФИО пациента")),
            "birth_date": _norm(original.get("Дата рождения")),
            "snils": _norm(original.get("СНИЛС")),
        }
        if not _is_same_patient(patient, cand):
            continue

        visit_time_val = data.get("Дата записи") or ""
        visit_dt = _parse_visit_time(visit_time_val)
        if not visit_dt:
            continue
        if visit_dt.tzinfo is None:
            visit_dt = visit_dt.replace(tzinfo=now.tzinfo)
        if visit_dt < now:
            continue

        result.append(
            {
                "Book_Id_Mis": _norm(data.get("Book_Id_Mis")),
                "MO_Name": _norm(data.get("Мед учреждение")),
                "MO_Adress": _norm(data.get("Адрес мед учреждения")),
                "Specialist_Name": _norm(data.get("ФИО врача")),
                "Room": _norm(data.get("Room")),
                "VisitTime": _norm(data.get("Дата записи")),
                "PatientSource": "local_db",
            }
        )
    return sorted(result, key=lambda x: _norm(x.get("VisitTime")))


async def _get_patient_id_from_soap(patient: Dict[str, Any], phone: str) -> Optional[str]:
    if not SOAP_URL:
        return None

    fio_parts = _norm(patient.get("fio")).split()
    while len(fio_parts) < 3:
        fio_parts.append("")
    sex_val = _gender_to_soap(patient.get("gender"))

    xml = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <GetPatientInfoRequest xmlns="http://www.rt-eu.ru/med/er/v2_0">
            <Session_ID>{str(uuid.uuid4())}</Session_ID>
            <Patient_Data>
                <OMS_Number>{_norm(patient.get("oms"))}</OMS_Number>
                <SNILS>{_norm(patient.get("snils"))}</SNILS>
                <First_Name>{_norm(fio_parts[1])}</First_Name>
                <Last_Name>{_norm(fio_parts[0])}</Last_Name>
                <Middle_Name>{_norm(fio_parts[2])}</Middle_Name>
                <Birth_Date>{_normalize_birth_date(_norm(patient.get("birth_date")))}</Birth_Date>
                <Phone>{_norm(phone)}</Phone>
                <Sex>{sex_val}</Sex>
            </Patient_Data>
            <Pass_referral>0</Pass_referral>
        </GetPatientInfoRequest>
    </soapenv:Body>
</soapenv:Envelope>"""

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "GetPatientInfo",
    }
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        _log_mis_io(
            "mis_get_patient_info_request",
            endpoint=SOAP_URL,
            soap_action="GetPatientInfo",
            patient_fio=_norm(patient.get("fio")),
            payload=xml,
        )
        async with aiohttp.ClientSession() as session:
            async with session.post(SOAP_URL, data=xml.encode("utf-8"), headers=headers, timeout=timeout) as resp:
                text = await resp.text()
                _log_mis_io(
                    "mis_get_patient_info_response",
                    endpoint=SOAP_URL,
                    status=resp.status,
                    patient_fio=_norm(patient.get("fio")),
                    response=text,
                )
                if resp.status < 200 or resp.status >= 300:
                    return None
                patient_id = SoapResponseParser.parse_patient_id(text)
                _log_mis_io(
                    "mis_get_patient_info_parsed",
                    patient_fio=_norm(patient.get("fio")),
                    patient_id=_norm(patient_id),
                )
                return patient_id
    except Exception as e:
        _log_mis_io(
            "mis_get_patient_info_error",
            endpoint=SOAP_URL,
            patient_fio=_norm(patient.get("fio")),
            error=str(e),
        )
        return None


async def _fetch_mis_appointments(patient_id: str) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    if not SOAP_URL_PATIENT_ID:
        return None, "mis_unavailable"
    url = f"{SOAP_URL_PATIENT_ID}?Status=1&PatientID={patient_id}"
    try:
        timeout = aiohttp.ClientTimeout(total=20)
        _log_mis_io(
            "mis_informer_request",
            endpoint=SOAP_URL_PATIENT_ID,
            full_url=url,
            patient_id=_norm(patient_id),
            status=1,
        )
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=timeout) as resp:
                if resp.status < 200 or resp.status >= 300:
                    text = await resp.text()
                    _log_mis_io(
                        "mis_informer_response",
                        endpoint=SOAP_URL_PATIENT_ID,
                        full_url=url,
                        patient_id=_norm(patient_id),
                        status=resp.status,
                        response=text,
                    )
                    return None, "mis_unavailable"
                data = await resp.json()
                arr = data.get("InformerResult", [])
                if not isinstance(arr, list):
                    arr = []
                _log_mis_io(
                    "mis_informer_response",
                    endpoint=SOAP_URL_PATIENT_ID,
                    full_url=url,
                    patient_id=_norm(patient_id),
                    status=resp.status,
                    records_count=len(arr),
                    response=data,
                )
                return arr, None
    except Exception as e:
        _log_mis_io(
            "mis_informer_error",
            endpoint=SOAP_URL_PATIENT_ID,
            full_url=url,
            patient_id=_norm(patient_id),
            error=str(e),
        )
        return None, "mis_unavailable"


def _filter_future(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    now = datetime.now().astimezone()
    out: List[Dict[str, Any]] = []
    for r in records:
        visit = _parse_visit_time(_norm(r.get("VisitTime")))
        if not visit:
            continue
        if visit.tzinfo is None:
            visit = visit.replace(tzinfo=now.tzinfo)
        if visit >= now:
            out.append(r)
    out.sort(key=lambda x: _norm(x.get("VisitTime")))
    return out


def _build_patients_keyboard(patients: List[Dict[str, Any]]) -> Any:
    rows: List[List[Dict[str, str]]] = []
    for idx, p in enumerate(patients):
        title = f"{_norm(p.get('fio'))} ({_norm(p.get('birth_date'))})"
        if _norm(p.get("source")) == "local":
            title += " • из ваших записей"
        rows.append([{"type": "callback", "text": title[:60], "payload": f"myapps_select_patient:{idx}"}])
    rows.append([{"type": "callback", "text": "🏠 Главное меню", "payload": "back_to_main"}])
    return create_keyboard(rows)


def _build_appointments_keyboard(appointments: List[Dict[str, Any]], page: int) -> Any:
    total = len(appointments)
    start = page * PAGE_SIZE
    end = min(total, start + PAGE_SIZE)
    rows: List[List[Dict[str, str]]] = []

    for i, app in enumerate(appointments[start:end], start=1):
        book_id = _norm(app.get("Book_Id_Mis"))
        if not book_id:
            continue
        rows.append(
            [
                {
                    "type": "callback",
                    "text": f"❌ Отменить запись #{start + i}",
                    "payload": f"cancel_mis:{book_id}",
                }
            ]
        )

    nav: List[Dict[str, str]] = []
    if page > 0:
        nav.append({"type": "callback", "text": "⬅️ Назад", "payload": f"myapps_page:{page-1}"})
    if end < total:
        nav.append({"type": "callback", "text": "➡️ Далее", "payload": f"myapps_page:{page+1}"})
    if nav:
        rows.append(nav)

    rows.append([{"type": "callback", "text": "👥 Выбрать пациента", "payload": "myapps_pick_patient"}])
    rows.append([{"type": "callback", "text": "🏠 Главное меню", "payload": "back_to_main"}])
    return create_keyboard(rows)


def _format_appointments_text(appointments: List[Dict[str, Any]], patient: Dict[str, Any], page: int) -> str:
    total = len(appointments)
    start = page * PAGE_SIZE
    end = min(total, start + PAGE_SIZE)
    chunk = appointments[start:end]
    lines = [f"📋 Записи к врачу\nПациент: {_norm(patient.get('fio'))}\n"]
    for idx, app in enumerate(chunk, start=1):
        global_idx = start + idx
        lines.append(
            f"Запись #{global_idx}\n"
            f"🏥 {_norm(app.get('MO_Name'))}\n"
            f"👨‍⚕️ {_norm(app.get('Specialist_Name'))}\n"
            f"🚪 {_norm(app.get('Room'))}\n"
            f"🗓 {_norm(app.get('VisitTime'))}"
        )
    lines.append(f"\nСтраница {page + 1} из {max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)}")
    return "\n\n".join(lines)


async def _mark_cancelled_locally(user_id: int, book_id_mis: str) -> None:
    try:
        db.cursor.execute(
            """
            SELECT id
            FROM appointments
            WHERE user_id = %s
              AND book_id_mis = %s
              AND status = 'active'
            LIMIT 1
            """,
            (user_id, book_id_mis),
        )
        row = db.cursor.fetchone()
        if not row:
            return
        appointment_id = int(row[0])
        db.cursor.execute(
            """
            UPDATE appointments
            SET status = 'cancelled',
                cancelled_at = NOW(),
                cancelled_by = 'user_cancel'
            WHERE id = %s
            """,
            (appointment_id,),
        )
        db.conn.commit()
    except Exception as e:
        if db.conn:
            db.conn.rollback()
        log_system_event("my_appointments", "mark_cancelled_failed", user_id=user_id, error=str(e))


async def _sync_live_records_to_db(user_id: int, records: List[Dict[str, Any]]) -> None:
    for record in records:
        visit_time = _parse_visit_time(_norm(record.get("VisitTime")))
        if not visit_time:
            continue
        data = {
            "Дата записи": _norm(record.get("VisitTime")),
            "Мед учреждение": _norm(record.get("MO_Name")),
            "Адрес мед учреждения": _norm(record.get("MO_Adress")),
            "ФИО врача": _norm(record.get("Specialist_Name")),
            "Должность врача": "",
            "Book_Id_Mis": _norm(record.get("Book_Id_Mis")),
            "PatientID": _norm(record.get("PatientID")),
            "Room": _norm(record.get("Room")),
            "Исходные_данные": {
                "ФИО пациента": " ".join(
                    x for x in [_norm(record.get("Last_Name")), _norm(record.get("First_Name")), _norm(record.get("Middle_Name"))] if x
                ),
                "Дата рождения": _normalize_birth_date(_norm(record.get("Birth_Date"))),
                "Телефон": _norm(record.get("Mobile_Phone")),
            },
        }
        db.add_appointment(user_id=user_id, appointment_data=data, booking_source="external")


async def send_my_appointments(bot: Bot, user_id: int, chat_id: int) -> None:
    """
    Старт сценария "Записи к врачу":
    выбираем пациента и затем показываем записи.
    """
    user_data = db.get_user_full_data(user_id) or {}
    phone = _norm(user_data.get("phone"))
    patients = await get_patients_by_phone(phone)
    for p in patients:
        p["source"] = "api"

    self_patient = {
        "fio": _norm(user_data.get("fio")),
        "birth_date": _norm(user_data.get("birth_date")),
        "snils": _norm(user_data.get("snils")),
        "oms": _norm(user_data.get("oms")),
        "gender": _norm(user_data.get("gender")),
        "source": "api",
    }
    if self_patient["fio"] and not any(_is_same_patient(self_patient, p) for p in patients):
        patients.insert(0, self_patient)

    local_only = _extract_local_patients(user_id)
    for lp in local_only:
        if not any(_is_same_patient(lp, ap) for ap in patients):
            patients.append(lp)

    if not patients:
        await bot.send_message(
            chat_id=chat_id,
            text="Записей к врачу не найдено, либо вы записывали пациента, у которого мед карта не привязана к Вашему номеру телефона!",
            attachments=[create_keyboard([[{"type": "callback", "text": "🏠 Главное меню", "payload": "back_to_main"}]])],
        )
        return

    _sessions[user_id] = {
        "patients": patients,
        "selected_patient": None,
        "appointments": [],
        "page": 0,
    }
    keyboard = _build_patients_keyboard(patients)
    await bot.send_message(
        chat_id=chat_id,
        text="Выберите пациента, для которого показать записи к врачу:",
        attachments=[keyboard] if keyboard else [],
    )


async def handle_my_appointments_callback(bot: Bot, user_id: int, chat_id: int, payload: str) -> bool:
    state = _sessions.get(user_id)
    if payload == "myapps_pick_patient":
        if not state:
            await send_my_appointments(bot, user_id, chat_id)
            return True
        keyboard = _build_patients_keyboard(state.get("patients", []))
        await bot.send_message(chat_id=chat_id, text="Выберите пациента:", attachments=[keyboard] if keyboard else [])
        return True

    if payload.startswith("myapps_select_patient:"):
        if not state:
            await send_my_appointments(bot, user_id, chat_id)
            return True
        try:
            idx = int(payload.split(":", 1)[1])
            patient = state["patients"][idx]
        except Exception:
            await bot.send_message(chat_id=chat_id, text="Не удалось определить пациента. Попробуйте снова.")
            return True

        state["selected_patient"] = patient
        owner = db.get_user_full_data(user_id) or {}
        phone = _norm(owner.get("phone"))

        patient_id = await _get_patient_id_from_soap(patient, phone)
        live_records: List[Dict[str, Any]] = []
        if patient_id:
            data, err = await _fetch_mis_appointments(patient_id)
            if err:
                await bot.send_message(chat_id=chat_id, text="Сервис записей временно недоступен")
                return True
            live_records = _filter_future(data or [])
            await _sync_live_records_to_db(user_id, live_records)

        if not live_records and _norm(patient.get("source")) == "local":
            live_records = _filter_future(_extract_local_appointments(user_id, patient))

        if not live_records:
            keyboard = create_keyboard([[{"type": "callback", "text": "👥 Выбрать пациента", "payload": "myapps_pick_patient"}], [{"type": "callback", "text": "🏠 Главное меню", "payload": "back_to_main"}]])
            await bot.send_message(
                chat_id=chat_id,
                text="Записей к врачу не найдено, либо вы записывали пациента, у которого мед карта не привязана к Вашему номеру телефона!",
                attachments=[keyboard] if keyboard else [],
            )
            return True

        state["appointments"] = live_records
        state["page"] = 0
        text = _format_appointments_text(live_records, patient, 0)
        keyboard = _build_appointments_keyboard(live_records, 0)
        await bot.send_message(chat_id=chat_id, text=text, attachments=[keyboard] if keyboard else [])
        log_user_event(user_id, "my_appointments_shown", count=len(live_records))
        return True

    if payload.startswith("myapps_page:"):
        if not state or not state.get("appointments"):
            await bot.send_message(chat_id=chat_id, text="Список записей устарел. Откройте «Записи к врачу» заново.")
            return True
        try:
            page = int(payload.split(":", 1)[1])
        except Exception:
            return True
        if page < 0:
            page = 0
        state["page"] = page
        apps = state.get("appointments", [])
        patient = state.get("selected_patient") or {}
        text = _format_appointments_text(apps, patient, page)
        keyboard = _build_appointments_keyboard(apps, page)
        await bot.send_message(chat_id=chat_id, text=text, attachments=[keyboard] if keyboard else [])
        return True

    if payload.startswith("cancel_mis:"):
        book_id_mis = _norm(payload.split(":", 1)[1])
        if not book_id_mis:
            await bot.send_message(chat_id=chat_id, text="Не удалось отменить запись: отсутствует Book_Id_Mis.")
            return True
        keyboard = create_keyboard(
            [
                [
                    {"type": "callback", "text": "✅ Да", "payload": f"cancel_mis_confirm:{book_id_mis}"},
                    {"type": "callback", "text": "⬅️ Назад", "payload": f"myapps_page:{(_sessions.get(user_id, {}).get('page', 0))}"},
                ]
            ]
        )
        await bot.send_message(
            chat_id=chat_id,
            text="⚠️ Вы подтверждаете отмену записи?",
            attachments=[keyboard] if keyboard else [],
        )
        return True

    if payload.startswith("cancel_mis_confirm:"):
        book_id_mis = _norm(payload.split(":", 1)[1])
        cancel_service = CancelService()
        cancel_result = await cancel_service.send_cancel_request(
            book_id_mis=book_id_mis,
            canceled_reason=cancel_service.DEFAULT_REASON,
        )
        if not cancel_result.get("success"):
            await bot.send_message(chat_id=chat_id, text="❌ Не удалось отменить запись во внешней системе. Попробуйте позже.")
            return True

        response_text = _norm(cancel_result.get("response"))
        if "SUCCESS" not in response_text:
            await bot.send_message(chat_id=chat_id, text="❌ Внешняя система вернула ошибку отмены.")
            return True

        await _mark_cancelled_locally(user_id, book_id_mis)

        state = _sessions.get(user_id) or {}
        old_apps = state.get("appointments") or []
        state["appointments"] = [a for a in old_apps if _norm(a.get("Book_Id_Mis")) != book_id_mis]
        _sessions[user_id] = state
        await bot.send_message(chat_id=chat_id, text="✅ Запись была отменена.")

        if state["appointments"]:
            page = min(state.get("page", 0), (len(state["appointments"]) - 1) // PAGE_SIZE)
            state["page"] = page
            keyboard = _build_appointments_keyboard(state["appointments"], page)
            text = _format_appointments_text(state["appointments"], state.get("selected_patient") or {}, page)
            await bot.send_message(chat_id=chat_id, text=text, attachments=[keyboard] if keyboard else [])
        return True

    return False

