# referral_visit/handlers.py
"""Обработчики сценария записи по направлению."""
import uuid
import re
from datetime import datetime, timedelta

from bot_utils import send_main_menu
from user_database import db
from logging_config import log_user_event, log_data_event

from referral_visit.states import ReferralUserContext
from referral_visit import keyboards as ref_kb
from referral_visit.soap_client import (
    get_patient_referrals,
    get_referral_by_number,
    get_mos,
    get_doctors_referral,
    get_slots_referral,
    book_appointment_referral,
)
from referral_visit.soap_parser import (
    parse_referrals,
    parse_get_referral_info_response,
    parse_session_id,
    parse_mo_list,
    parse_doctors,
    parse_slots,
    parse_create_appointment_details,
)
from referral_visit.constants import (
    NO_SLOTS_MESSAGE,
    GET_REFERRAL_ERROR_MESSAGES,
    INACTIVITY_TIMEOUT_MINUTES,
    SOAP_SESSION_TIMEOUT_MINUTES,
)
from visit_a_doctor.specialties_mapping import get_specialty_name
async def _check_referral_validity(bot, user_id: int, chat_id: int, ctx: ReferralUserContext) -> bool:
    """Проверка таймаута и сессии для сценария направления. Возвращает False если нужно прервать."""
    if ctx.is_expired(INACTIVITY_TIMEOUT_MINUTES):
        referral_user_states.pop(user_id, None)
        referral_cache.pop(user_id, None)
        await bot.send_message(
            chat_id=chat_id,
            text="⏱️ Сессия истекла из-за неактивности. Начните запись по направлению заново.",
            attachments=[ref_kb.kb_restart()],
        )
        return False
    if ctx.session_id and ctx.is_session_expired(SOAP_SESSION_TIMEOUT_MINUTES):
        referral_user_states.pop(user_id, None)
        referral_cache.pop(user_id, None)
        await bot.send_message(
            chat_id=chat_id,
            text="⏱️ Сессия авторизации истекла. Начните запись по направлению заново.",
            attachments=[ref_kb.kb_restart()],
        )
        return False
    return True


async def _referral_soap_error(bot, user_id: int, chat_id: int, error_msg: str):
    """Ошибка SOAP в сценарии направления: своя очистка и кнопка ref_restart."""
    referral_user_states.pop(user_id, None)
    referral_cache.pop(user_id, None)
    await bot.send_message(
        chat_id=chat_id,
        text="❌ Ошибка связи с системой записи. Начните запись по направлению заново.",
        attachments=[ref_kb.kb_restart()],
    )
    log_user_event(user_id, "referral_soap_error", error=error_msg)

referral_user_states: dict = {}
referral_cache: dict = {}


def get_ctx(user_id: int) -> ReferralUserContext:
    if user_id not in referral_user_states:
        referral_user_states[user_id] = ReferralUserContext(user_id=user_id)
    referral_user_states[user_id].update_activity()
    return referral_user_states[user_id]


def get_ref_cache(user_id: int) -> dict:
    if user_id not in referral_cache:
        referral_cache[user_id] = {}
    return referral_cache[user_id]


def _referral_dates_start_end(ctx: ReferralUserContext) -> tuple:
    """Возвращает (start_date, end_date) в формате YYYY-MM-DD для запросов МИС."""
    today = datetime.now().date()
    start_s = ctx.referral_start_date or ""
    end_s = ctx.referral_end_date or ""
    if not start_s:
        start_d = today
    else:
        try:
            start_d = datetime.strptime(start_s[:10], "%Y-%m-%d").date()
        except Exception:
            start_d = today
    end_max = today + timedelta(days=14)
    if not end_s:
        end_d = end_max
    else:
        try:
            end_d = datetime.strptime(end_s[:10], "%Y-%m-%d").date()
        except Exception:
            end_d = end_max
    start_d = max(start_d, today)
    end_d = min(end_d, end_max)
    if start_d > end_d:
        return (today.strftime("%Y-%m-%d"), (today + timedelta(days=14)).strftime("%Y-%m-%d"))
    return (start_d.strftime("%Y-%m-%d"), end_d.strftime("%Y-%m-%d"))


def _format_referral_display(r: dict, mo_name: str) -> str:
    """Форматирует строку для кнопки: Направление №X от DD.MM.YYYY (до ...) специальность МО."""
    num = r.get("referral_number", r.get("referral_id", ""))
    start = r.get("referral_start_date", "")[:10]
    end = r.get("referral_end_date", "")[:10]
    if start:
        try:
            dt = datetime.strptime(start, "%Y-%m-%d")
            start = dt.strftime("%d.%m.%Y")
        except Exception:
            pass
    if end:
        try:
            dt = datetime.strptime(end, "%Y-%m-%d")
            end = dt.strftime("%d.%m.%Y")
        except Exception:
            pass
    spec = ""
    if r.get("post_id"):
        spec = get_specialty_name(r["post_id"])
    elif r.get("specialty_id"):
        spec = get_specialty_name(r["specialty_id"])
    if not spec:
        spec = "по направлению"
    parts = [f"Направление №{num} от {start}"]
    if end:
        parts.append(f"(до {end})")
    parts.append(spec)
    parts.append(mo_name or "")
    return " ".join(p for p in parts if p).strip()


async def start_referral_booking(bot, user_id: int, chat_id: int):
    """Старт сценария: только запись себя, данные из БД → подтверждение → GetPatientInfo(Pass_referral=1)."""
    ctx = ReferralUserContext(user_id=user_id)
    referral_user_states[user_id] = ctx
    referral_cache[user_id] = {}
    ctx.step = "REF_PERSON"
    ctx.selected_person = "me"
    ctx.update_activity()

    user_data = db.get_user_full_data(user_id)
    if not user_data:
        await bot.send_message(
            chat_id=chat_id,
            text="❌ Не удалось получить данные профиля. Попробуйте позже.",
            attachments=[ref_kb.kb_restart()],
        )
        return

    ctx.patient_fio = user_data.get("fio", "")
    ctx.patient_birthdate = user_data.get("birth_date", "")
    ctx.patient_gender = user_data.get("gender", "")
    ctx.patient_snils = user_data.get("snils", "")
    ctx.patient_oms = user_data.get("oms", "")

    if not all([ctx.patient_fio, ctx.patient_birthdate, ctx.patient_gender, ctx.patient_snils, ctx.patient_oms]):
        await bot.send_message(
            chat_id=chat_id,
            text="❌ Для записи по направлению нужны полные данные профиля (ФИО, дата рождения, пол, СНИЛС, полис ОМС). Заполните их в разделе регистрации.",
            attachments=[ref_kb.kb_restart()],
        )
        return

    ctx.step = "REF_PATIENT_CONFIRM"
    summary = (
        "ℹ️ Проверьте данные для записи по направлению:\n\n"
        f"ФИО: {ctx.patient_fio}\n"
        f"Дата рождения: {ctx.patient_birthdate}\n"
        f"Полис ОМС: {ctx.patient_oms}\n\n"
        "Нажмите «Продолжить», чтобы загрузить ваши направления."
    )
    keyboard = create_keyboard([
        [{"type": "callback", "text": "✅ Продолжить", "payload": "ref_confirm_patient"}],
        [ref_kb.get_back_button("back_to_main")],
    ])
    await bot.send_message(chat_id=chat_id, text=summary, attachments=[keyboard])


def create_keyboard(buttons):
    from bot_utils import create_keyboard as ck
    return ck(buttons)


async def _load_referrals_and_show_list(bot, user_id: int, chat_id: int, ctx: ReferralUserContext):
    """После подтверждения пациента: GetPatientInfo(Pass_referral=1), парсим направления, грузим МО, показываем список."""
    ctx.client_session_id = str(uuid.uuid4())
    parts = ctx.patient_fio.split()
    if len(parts) < 3:
        parts = ["Иванов", "Иван", "Иванович"]
    birthdate_iso = ctx.patient_birthdate
    if "." in ctx.patient_birthdate:
        birthdate_iso = "-".join(ctx.patient_birthdate.split(".")[::-1])

    await bot.send_message(chat_id=chat_id, text="🔄 Загрузка направлений...")
    try:
        xml = await get_patient_referrals(
            snils=ctx.patient_snils,
            oms=ctx.patient_oms,
            birthdate=birthdate_iso,
            fio_parts=parts,
            gender=ctx.patient_gender,
            client_session_id=ctx.client_session_id,
        )
    except Exception as e:
        await bot.send_message(
            chat_id=chat_id,
            text=f"❌ Ошибка связи с системой записи. Попробуйте позже.",
            attachments=[ref_kb.kb_restart()],
        )
        log_user_event(user_id, "referral_soap_error", error=str(e))
        return

    session_id = parse_session_id(xml)
    if not session_id:
        await bot.send_message(
            chat_id=chat_id,
            text="❌ Ошибка авторизации. Проверьте данные и попробуйте снова.",
            attachments=[ref_kb.kb_restart()],
        )
        return

    ctx.session_id = session_id
    ctx.session_created_at = datetime.now()

    referrals = parse_referrals(xml)
    if not referrals:
        await bot.send_message(
            chat_id=chat_id,
            text=NO_SLOTS_MESSAGE,
            attachments=[ref_kb.kb_no_referrals_find()],
        )
        ctx.step = "REF_LIST"
        ctx.referrals = []
        return

    try:
        xml_mo = await get_mos(session_id)
        mos = parse_mo_list(xml_mo)
    except Exception:
        mos = []
    cache = get_ref_cache(user_id)
    cache["mos"] = mos

    mo_by_oid = {}
    for m in mos:
        oid = m.get("oid", "")
        if oid:
            mo_by_oid[oid] = m

    for r in referrals:
        r["display_text"] = _format_referral_display(r, mo_by_oid.get(r.get("to_mo_oid", ""), {}).get("name", ""))

    ctx.referrals = referrals
    ctx.step = "REF_LIST"
    ctx.ref_list_page = 0
    msg = "📄 Выберите направление для записи:\n\n"
    for i, r in enumerate(referrals[:10], 1):
        msg += f"{i}. {r.get('display_text', r.get('referral_number', ''))}\n\n"
    if len(referrals) > 10:
        msg += "..."

    keyboard = ref_kb.kb_referral_list(referrals, 0)
    await bot.send_message(chat_id=chat_id, text=msg or "Выберите направление:", attachments=[keyboard])


async def handle_referral_callback(bot, user_id: int, chat_id: int, payload: str):
    """Обработка callback ref_*."""
    ctx = get_ctx(user_id)
    cache = get_ref_cache(user_id)

    if payload != "ref_restart" and ctx.session_id:
        if not await _check_referral_validity(bot, user_id, chat_id, ctx):
            return

    if payload == "ref_restart":
        await start_referral_booking(bot, user_id, chat_id)
        return

    if payload == "ref_confirm_patient":
        if ctx.step != "REF_PATIENT_CONFIRM":
            return
        await _load_referrals_and_show_list(bot, user_id, chat_id, ctx)
        return

    if payload == "ref_find_by_number":
        ctx.step = "REF_ENTER_NUMBER"
        await bot.send_message(
            chat_id=chat_id,
            text="Введите номер направления:",
            attachments=[ref_kb.kb_enter_referral_number_back()],
        )
        return

    if payload == "ref_back_to_list":
        ctx.step = "REF_LIST"
        if not ctx.referrals:
            await bot.send_message(
                chat_id=chat_id,
                text=NO_SLOTS_MESSAGE,
                attachments=[ref_kb.kb_no_referrals_find()],
            )
        else:
            keyboard = ref_kb.kb_referral_list(ctx.referrals, ctx.ref_list_page)
            await bot.send_message(chat_id=chat_id, text="Выберите направление:", attachments=[keyboard])
        return

    if payload.startswith("ref_list_page_"):
        try:
            page = int(payload.replace("ref_list_page_", ""))
            ctx.ref_list_page = page
            keyboard = ref_kb.kb_referral_list(ctx.referrals, page)
            await bot.send_message(chat_id=chat_id, text="Выберите направление:", attachments=[keyboard])
        except ValueError:
            pass
        return

    if payload.startswith("ref_sel_"):
        ref_id = payload.replace("ref_sel_", "")
        selected = next((r for r in ctx.referrals if r.get("referral_id") == ref_id), None)
        if not selected:
            await bot.send_message(chat_id=chat_id, text="Направление не найдено.", attachments=[ref_kb.kb_restart()])
            return

        ctx.selected_referral = selected
        ctx.selected_referral_id = ref_id
        ctx.selected_referral_number = selected.get("referral_number", "")
        ctx.referral_start_date = selected.get("referral_start_date", "")[:10]
        ctx.referral_end_date = (selected.get("referral_end_date") or "")[:10]
        ctx.selected_mo_oid = selected.get("to_mo_oid", "")
        ctx.selected_post_id = selected.get("post_id", "")
        ctx.selected_specialty_id = selected.get("specialty_id", "")
        ctx.selected_service_id = selected.get("service_id", "")

        mos = cache.get("mos", [])
        mo = next((m for m in mos if (m.get("oid") or "") == ctx.selected_mo_oid), None)
        if mo:
            ctx.selected_mo_id = mo.get("id", "")
            ctx.selected_mo_name = mo.get("name", "")

        to_snils = (selected.get("to_resource_snils") or "").strip()
        to_name = (selected.get("to_resource_name") or "").strip()
        if to_snils and to_name:
            ctx.selected_doctor_id = to_snils
            ctx.selected_doctor_name = to_name
            ctx.selected_resource_type = "specialist"
            if not ctx.selected_post_id:
                await bot.send_message(
                    chat_id=chat_id,
                    text=NO_SLOTS_MESSAGE,
                    attachments=[ref_kb.kb_referral_list(ctx.referrals, ctx.ref_list_page)],
                )
                return
            start_d, end_d = _referral_dates_start_end(ctx)
            await bot.send_message(chat_id=chat_id, text="🔄 Поиск доступных дат...")
            try:
                xml_res = await get_doctors_referral(ctx.session_id, ctx.selected_post_id, ctx.selected_mo_oid, start_d, end_d)
                doctors = parse_doctors(xml_res)
            except Exception as e:
                await _referral_soap_error(bot, user_id, chat_id, str(e))
                return
            doc = next((d for d in doctors if d.get("id") == to_snils), None)
            if doc and doc.get("mo_oid"):
                ctx.selected_mo_oid = doc["mo_oid"]
            ctx.available_dates_cache = doc.get("dates", []) if doc else []
            if not ctx.available_dates_cache:
                await bot.send_message(chat_id=chat_id, text=NO_SLOTS_MESSAGE, attachments=[ref_kb.kb_referral_list(ctx.referrals, ctx.ref_list_page)])
                return
            ctx.step = "REF_DATE"
            ctx.date_page = 0
            keyboard = ref_kb.kb_date_selection(ctx.available_dates_cache, 0)
            await bot.send_message(chat_id=chat_id, text="Выберите дату приёма:", attachments=[keyboard])
            return

        if not ctx.selected_post_id:
            await bot.send_message(
                chat_id=chat_id,
                text="Запись по данному типу направления (обследование) временно недоступна. Обратитесь в поликлинику.",
                attachments=[ref_kb.kb_referral_list(ctx.referrals, ctx.ref_list_page)],
            )
            return

        start_d, end_d = _referral_dates_start_end(ctx)
        await bot.send_message(chat_id=chat_id, text="🔄 Поиск врачей и кабинетов...")
        try:
            xml_res = await get_doctors_referral(ctx.session_id, ctx.selected_post_id, ctx.selected_mo_oid, start_d, end_d)
            doctors = parse_doctors(xml_res)
        except Exception as e:
            await _referral_soap_error(bot, user_id, chat_id, str(e))
            return
        if not doctors:
            await bot.send_message(chat_id=chat_id, text=NO_SLOTS_MESSAGE, attachments=[ref_kb.kb_referral_list(ctx.referrals, ctx.ref_list_page)])
            return
        cache["doctors"] = doctors
        ctx.selected_spec = get_specialty_name(ctx.selected_post_id)
        ctx.step = "REF_DOCTOR"
        keyboard = ref_kb.kb_doctor_selection(doctors)
        await bot.send_message(chat_id=chat_id, text=f"Выберите врача или кабинет ({ctx.selected_spec}):", attachments=[keyboard])
        return

    if payload.startswith("ref_doc_"):
        doc_id = payload.replace("ref_doc_", "")
        doctors = cache.get("doctors", [])
        found = next((d for d in doctors if d.get("id") == doc_id), None)
        if not found:
            return
        ctx.selected_doctor_id = doc_id
        ctx.selected_doctor_name = found.get("name", "")
        ctx.selected_resource_type = found.get("type", "specialist")
        ctx.available_dates_cache = found.get("dates", [])
        # ВАЖНО: используем MO_OID подразделения ресурса для дальнейшего GetScheduleInfo
        mo_oid_for_doc = found.get("mo_oid")
        if mo_oid_for_doc:
            ctx.selected_mo_oid = mo_oid_for_doc
        if found.get("type") == "room":
            ctx.selected_room_id = found.get("room_id", "")
            ctx.selected_room_oid = found.get("room_oid", "")
        if not ctx.available_dates_cache:
            await bot.send_message(chat_id=chat_id, text=NO_SLOTS_MESSAGE, attachments=[ref_kb.kb_restart()])
            return
        ctx.step = "REF_DATE"
        ctx.date_page = 0
        keyboard = ref_kb.kb_date_selection(ctx.available_dates_cache, 0)
        await bot.send_message(chat_id=chat_id, text="Выберите дату приёма:", attachments=[keyboard])
        return

    if payload == "ref_back_to_doc":
        doctors = cache.get("doctors", [])
        ctx.step = "REF_DOCTOR"
        keyboard = ref_kb.kb_doctor_selection(doctors)
        await bot.send_message(chat_id=chat_id, text=f"Выберите врача или кабинет ({ctx.selected_spec}):", attachments=[keyboard])
        return

    if payload.startswith("ref_date_page_"):
        try:
            page = int(payload.replace("ref_date_page_", ""))
            ctx.date_page = page
            keyboard = ref_kb.kb_date_selection(ctx.available_dates_cache or [], page)
            await bot.send_message(chat_id=chat_id, text="Выберите дату:", attachments=[keyboard])
        except ValueError:
            pass
        return

    if payload.startswith("ref_date_"):
        date_str = payload.replace("ref_date_", "")
        ctx.selected_date = date_str
        specialist_snils = ctx.selected_doctor_id if ctx.selected_resource_type == "specialist" else ""
        room_id = ctx.selected_room_id if ctx.selected_resource_type == "room" else None
        room_oid = ctx.selected_room_oid if ctx.selected_resource_type == "room" else None
        try:
            xml_slots = await get_slots_referral(
                ctx.session_id,
                specialist_snils,
                ctx.selected_mo_oid,
                ctx.selected_post_id,
                date_str,
                room_id=room_id,
                room_oid=room_oid,
            )
            slots = parse_slots(xml_slots)
        except Exception as e:
            await _referral_soap_error(bot, user_id, chat_id, str(e))
            return
        if not slots:
            await bot.send_message(chat_id=chat_id, text=NO_SLOTS_MESSAGE, attachments=[ref_kb.kb_restart()])
            return
        cache["slots"] = slots
        ctx.available_slots_cache = slots
        ctx.step = "REF_TIME"
        ctx.time_page = 0
        keyboard = ref_kb.kb_time_selection(slots, 0)
        await bot.send_message(chat_id=chat_id, text=f"Выберите время на {date_str}:", attachments=[keyboard])
        return

    if payload == "ref_back_to_date":
        ctx.step = "REF_DATE"
        keyboard = ref_kb.kb_date_selection(ctx.available_dates_cache or [], ctx.date_page)
        await bot.send_message(chat_id=chat_id, text="Выберите дату приёма:", attachments=[keyboard])
        return

    if payload.startswith("ref_time_page_"):
        try:
            page = int(payload.replace("ref_time_page_", ""))
            ctx.time_page = page
            slots = cache.get("slots", [])
            keyboard = ref_kb.kb_time_selection(slots, page)
            await bot.send_message(chat_id=chat_id, text="Выберите время:", attachments=[keyboard])
        except ValueError:
            pass
        return

    if payload.startswith("ref_time_"):
        slot_id = payload.replace("ref_time_", "")
        slots = cache.get("slots", [])
        found = next((s for s in slots if s.get("id") == slot_id), None)
        if not found:
            return
        ctx.selected_slot_id = slot_id
        ctx.selected_time = found.get("time", "")
        ctx.selected_room = found.get("room", "")
        ctx.step = "REF_CONFIRM"
        try:
            from visit_a_doctor.specialties_MO import Abbreviations_MO
            mo_short = Abbreviations_MO.get(ctx.selected_mo_name, ctx.selected_mo_name)
        except Exception:
            mo_short = ctx.selected_mo_name
        confirm_text = (
            f"ℹ️ Подтверждение записи по направлению №{ctx.selected_referral_number}\n\n"
            f"🏥 МО: {mo_short}\n"
            f"👨‍⚕️ Врач: {ctx.selected_doctor_name} ({ctx.selected_spec})\n"
            f"🚪 Кабинет: {ctx.selected_room}\n"
            f"🗓 Дата: {ctx.selected_date}\n"
            f"⏰ Время: {ctx.selected_time}\n\n"
            "Подтвердить запись?"
        )
        await bot.send_message(chat_id=chat_id, text=confirm_text, attachments=[ref_kb.kb_confirm_referral_booking()])
        return

    if payload == "ref_back_to_time":
        ctx.step = "REF_TIME"
        slots = cache.get("slots", [])
        keyboard = ref_kb.kb_time_selection(slots, ctx.time_page)
        await bot.send_message(chat_id=chat_id, text="Выберите время:", attachments=[keyboard])
        return

    if payload == "ref_confirm_booking":
        await bot.send_message(chat_id=chat_id, text="🔄 Оформление записи...")
        try:
            xml = await book_appointment_referral(ctx.session_id, ctx.selected_slot_id)
            details = parse_create_appointment_details(xml)
            success = (details.get("status_code") or "").strip().upper() == "SUCCESS"
        except Exception as e:
            await _referral_soap_error(bot, user_id, chat_id, str(e))
            return

        if success:
            visit_time_str = details.get("visit_time") or f"{ctx.selected_date} {ctx.selected_time}:00"
            room_str = details.get("room") or ctx.selected_room or ""
            book_id_mis = details.get("book_id_mis") or ""
            mo_address = ""
            if cache.get("mos"):
                mo_obj = next((m for m in cache["mos"] if m.get("id") == ctx.selected_mo_id), None)
                if mo_obj:
                    mo_address = mo_obj.get("address", "")

            appointment_data = {
                "Дата записи": visit_time_str,
                "Мед учреждение": ctx.selected_mo_name,
                "Адрес мед учреждения": mo_address,
                "ФИО врача": ctx.selected_doctor_name,
                "Должность врача": ctx.selected_spec,
                "Book_Id_Mis": book_id_mis,
                "Slot_Id": ctx.selected_slot_id,
                "Room": room_str,
                "Referral_Number": ctx.selected_referral_number,
                "Исходные_данные": {
                    "ФИО пациента": ctx.patient_fio,
                    "Дата рождения": ctx.patient_birthdate,
                    "СНИЛС": ctx.patient_snils,
                    "ОМС": ctx.patient_oms,
                    "Пол": ctx.patient_gender,
                },
            }
            db.add_appointment(user_id, appointment_data, booking_source="self_bot")

            summary = (
                "✅ Запись по направлению успешно оформлена!\n\n"
                f"Направление №{ctx.selected_referral_number}\n"
                f"🏥 МО: {ctx.selected_mo_name}\n"
                f"👨‍⚕️ Врач: {ctx.selected_doctor_name}\n"
                f"🗓 Дата: {ctx.selected_date}\n"
                f"⏰ Время: {ctx.selected_time}\n\n"
                "За день до приёма вам придёт уведомление."
            )
            await bot.send_message(chat_id=chat_id, text=summary, attachments=[ref_kb.kb_final_menu()])
        else:
            code = (details.get("status_code") or "").strip().upper()
            comment = (details.get("comment") or "").strip()
            if code == "APPOINT_PATIENT_REGISTERED_SPECIALIST":
                err = comment or "Пациент уже записан к этому специалисту."
            elif code == "APPOINT_TIME_IS_BUSY":
                err = "Время уже занято. Выберите другое."
            else:
                err = "Не удалось оформить запись. Попробуйте другое время."
            await bot.send_message(chat_id=chat_id, text=f"❌ {err}", attachments=[ref_kb.kb_final_menu()])

        referral_user_states.pop(user_id, None)
        referral_cache.pop(user_id, None)


async def handle_referral_text_input(bot, user_id: int, chat_id: int, text: str) -> bool:
    """Обработка ввода номера направления (шаг REF_ENTER_NUMBER). Возвращает True если обработано."""
    if user_id not in referral_user_states:
        return False
    ctx = referral_user_states[user_id]
    if ctx.step != "REF_ENTER_NUMBER":
        return False

    ref_number = (text or "").strip()
    if not ref_number:
        await bot.send_message(chat_id=chat_id, text="Введите номер направления (число или текст с бланка).", attachments=[ref_kb.kb_enter_referral_number_back()])
        return True

    parts = ctx.patient_fio.split()
    if len(parts) < 3:
        parts = ["Иванов", "Иван", "Иванович"]
    birthdate_iso = ctx.patient_birthdate
    if "." in ctx.patient_birthdate:
        birthdate_iso = "-".join(ctx.patient_birthdate.split(".")[::-1])

    await bot.send_message(chat_id=chat_id, text="🔄 Поиск направления...")
    try:
        xml = await get_referral_by_number(
            session_id=ctx.session_id,
            snils=ctx.patient_snils,
            oms=ctx.patient_oms,
            birthdate=birthdate_iso,
            fio_parts=parts,
            gender=ctx.patient_gender,
            referral_number=ref_number,
        )
    except Exception as e:
        await bot.send_message(chat_id=chat_id, text="❌ Ошибка связи. Попробуйте позже.", attachments=[ref_kb.kb_enter_referral_number_back()])
        return True

    parsed = parse_get_referral_info_response(xml)
    if parsed.get("error_code"):
        code = parsed["error_code"]
        msg = GET_REFERRAL_ERROR_MESSAGES.get(code, GET_REFERRAL_ERROR_MESSAGES.get("UNDEFINED_ERROR", "Произошла ошибка."))
        await bot.send_message(chat_id=chat_id, text=msg, attachments=[ref_kb.kb_enter_referral_number_back()])
        if code == "SESSION_TIMED_OUT":
            referral_user_states.pop(user_id, None)
            referral_cache.pop(user_id, None)
        return True

    referrals = parsed.get("referrals", [])
    if not referrals:
        await bot.send_message(chat_id=chat_id, text=NO_SLOTS_MESSAGE, attachments=[ref_kb.kb_no_referrals_find()])
        ctx.step = "REF_LIST"
        ctx.referrals = []
        return True

    try:
        xml_mo = await get_mos(ctx.session_id)
        mos = parse_mo_list(xml_mo)
    except Exception:
        mos = []
    cache = get_ref_cache(user_id)
    cache["mos"] = mos
    mo_by_oid = {m.get("oid"): m for m in mos if m.get("oid")}
    for r in referrals:
        r["display_text"] = _format_referral_display(r, (mo_by_oid.get(r.get("to_mo_oid", "")) or {}).get("name", ""))
    ctx.referrals = referrals
    ctx.step = "REF_LIST"
    ctx.ref_list_page = 0
    keyboard = ref_kb.kb_referral_list(referrals, 0)
    await bot.send_message(chat_id=chat_id, text="Направление найдено. Выберите для записи:", attachments=[keyboard])
    return True
