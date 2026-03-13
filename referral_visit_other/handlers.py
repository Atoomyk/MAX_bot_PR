"""Обработчики сценария записи по направлению ДРУГОГО человека."""
import re
from datetime import datetime, timedelta
from typing import Dict, Any, List

from user_database import db
from logging_config import log_user_event, log_data_event
from visit_a_doctor.specialties_mapping import get_specialty_name

from referral_visit_other.states import ReferralOtherUserContext
from referral_visit_other import keyboards as ref_kb_other
from referral_visit_other.soap_client import (
    get_patient_session_other,
    get_referral_info_other,
    get_mos,
    get_doctors_referral,
    get_slots_referral,
    book_appointment_referral,
)
from referral_visit_other.soap_parser import (
    parse_session_id,
    parse_get_referral_info_response,
    parse_mo_list,
    parse_doctors,
    parse_slots,
    parse_create_appointment_details,
)
from referral_visit_other.constants import (
    INACTIVITY_TIMEOUT_MINUTES,
    SOAP_SESSION_TIMEOUT_MINUTES,
    NO_SLOTS_MESSAGE,
)
import uuid


other_states: Dict[int, ReferralOtherUserContext] = {}
other_cache: Dict[int, Dict[str, Any]] = {}


def get_ctx(user_id: int) -> ReferralOtherUserContext:
    if user_id not in other_states:
        other_states[user_id] = ReferralOtherUserContext(user_id=user_id)
    other_states[user_id].update_activity()
    return other_states[user_id]


def get_cache(user_id: int) -> Dict[str, Any]:
    if user_id not in other_cache:
        other_cache[user_id] = {}
    return other_cache[user_id]


def _dates_from_referral(ctx: ReferralOtherUserContext) -> tuple[str, str]:
    """Возвращает (start_date, end_date) в формате YYYY-MM-DD с ограничением по 14 дням, как в referral_visit."""
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


async def _check_ctx_validity(bot, user_id: int, chat_id: int, ctx: ReferralOtherUserContext) -> bool:
    if ctx.is_expired(INACTIVITY_TIMEOUT_MINUTES):
        other_states.pop(user_id, None)
        other_cache.pop(user_id, None)
        await bot.send_message(
            chat_id=chat_id,
            text="⏱️ Сессия истекла из-за неактивности. Начните запись по направлению заново.",
        )
        return False
    if ctx.session_id and ctx.is_session_expired(SOAP_SESSION_TIMEOUT_MINUTES):
        other_states.pop(user_id, None)
        other_cache.pop(user_id, None)
        await bot.send_message(
            chat_id=chat_id,
            text="⏱️ Сессия авторизации истекла. Начните запись по направлению заново.",
        )
        return False
    return True


async def _soap_error(bot, user_id: int, chat_id: int, error_msg: str):
    other_states.pop(user_id, None)
    other_cache.pop(user_id, None)
    await bot.send_message(
        chat_id=chat_id,
        text="❌ Ошибка связи с системой записи. Начните запись по направлению заново.",
    )
    log_user_event(user_id, "referral_other_soap_error", error=error_msg)


async def start_referral_other_booking(bot, user_id: int, chat_id: int):
    """
    Старт сценария записи по направлению другого человека.
    Пытаемся найти прикреплённых пациентов по телефону владельца.
    """
    ctx = ReferralOtherUserContext(user_id=user_id)
    ctx.step = "OTHER_PERSON_SELECT"
    other_states[user_id] = ctx
    other_cache[user_id] = {}
    ctx.update_activity()

    user_data = db.get_user_full_data(user_id)
    phone = user_data.get("phone", "") if user_data else ""

    if not phone:
        # Сразу ручной ввод
        ctx.step = "OTHER_ENTER_FIO"
        await bot.send_message(
            chat_id=chat_id,
            text="Пожалуйста, введите ФИО пациента.\n\nПример: Иванова Анна Максимовна",
        )
        return

    await bot.send_message(chat_id=chat_id, text="🔄 Проверяем список прикреплённых пациентов...")
    from patient_api_client import get_patients_by_phone

    try:
        found_patients = await get_patients_by_phone(phone)
    except Exception as e:
        log_data_event(user_id, "referral_other_patients_fetch_error", error=str(e))
        found_patients = []

    filtered: List[dict] = []
    if found_patients and user_data:
        user_snils_clean = re.sub(r"\D", "", user_data.get("snils", "") or "")
        for p in found_patients:
            p_snils_clean = re.sub(r"\D", "", p.get("snils", "") or "")
            if user_snils_clean and p_snils_clean and user_snils_clean == p_snils_clean:
                continue
            if (
                p.get("fio", "").lower() == (user_data.get("fio", "") or "").lower()
                and p.get("birth_date", "") == user_data.get("birth_date", "")
            ):
                continue
            filtered.append(p)

    if filtered:
        ctx.family_candidates = filtered
        ctx.update_activity()
        keyboard = ref_kb_other.kb_other_patient_candidates(filtered)
        await bot.send_message(
            chat_id=chat_id,
            text="📋 Выберите пациента из списка или введите данные вручную:",
            attachments=[keyboard] if keyboard else [],
        )
        return

    # Нет прикреплённых пациентов — ручной ввод
    ctx.step = "OTHER_ENTER_FIO"
    await bot.send_message(
        chat_id=chat_id,
        text="Пожалуйста, введите ФИО пациента.\n\nПример: Иванова Анна Максимовна",
    )


async def handle_referral_other_callback(bot, user_id: int, chat_id: int, payload: str):
    """
    Обработка callback-ов для сценария записи по направлению другого человека.
    payload начинается с ref_other_... или ref_person_other и т.п.
    """
    ctx = get_ctx(user_id)
    cache = get_cache(user_id)

    if payload != "ref_restart" and ctx.session_id:
        if not await _check_ctx_validity(bot, user_id, chat_id, ctx):
            return

    if payload == "ref_restart":
        other_states.pop(user_id, None)
        other_cache.pop(user_id, None)
        await start_referral_other_booking(bot, user_id, chat_id)
        return

    # Выбор пациента из списка / ручной ввод
    if payload.startswith("ref_other_select_"):
        selection_idx = payload.replace("ref_other_select_", "")
        if selection_idx == "manual":
            ctx.step = "OTHER_ENTER_FIO"
            ctx.is_from_rms = False
            await bot.send_message(
                chat_id=chat_id,
                text="Пожалуйста, введите ФИО пациента.\n\nПример: Иванова Анна Максимовна",
            )
            return
        try:
            idx = int(selection_idx)
            selected_p = ctx.family_candidates[idx]
        except (ValueError, IndexError):
            await bot.send_message(chat_id=chat_id, text="⚠ Ошибка выбора. Введите данные пациента вручную.")
            ctx.step = "OTHER_ENTER_FIO"
            await bot.send_message(chat_id=chat_id, text="Введите ФИО пациента:")
            return

        ctx.patient_fio = selected_p.get("fio", "")
        ctx.patient_birthdate = selected_p.get("birth_date", "")
        ctx.patient_snils = selected_p.get("snils", "") or ""
        ctx.patient_oms = selected_p.get("oms", "") or ""
        ctx.patient_gender = selected_p.get("gender", "") or ""
        ctx.is_from_rms = True
        ctx.step = "OTHER_CONFIRM_PATIENT"
        ctx.update_activity()

        await _show_other_patient_confirmation(bot, user_id, chat_id, ctx)
        return

    if payload == "ref_other_confirm_patient":
        # После подтверждения данных — авторизация и запрос номера направления
        await _authorize_other_and_request_ref_number(bot, user_id, chat_id, ctx)
        return

    if payload == "ref_back_to_other_patient":
        ctx.step = "OTHER_CONFIRM_PATIENT"
        await _show_other_patient_confirmation(bot, user_id, chat_id, ctx)
        return

    # Выбор подразделения МО по направлению
    if payload.startswith("ref_other_mo_sub_"):
        idx_str = payload.replace("ref_other_mo_sub_", "")
        try:
            idx = int(idx_str)
            subdivisions = cache.get("mo_subdivisions", [])
            chosen = subdivisions[idx]
        except (ValueError, IndexError):
            await bot.send_message(chat_id=chat_id, text="Ошибка выбора подразделения. Попробуйте снова.")
            return
        ctx.selected_mo_id = chosen.get("id", "")
        ctx.selected_mo_oid = chosen.get("oid", "")
        ctx.selected_mo_name = chosen.get("name", "")
        await _after_mo_selected_for_referral(bot, user_id, chat_id, ctx, cache)
        return

    # Остальная часть сценария (выбор направления, врача, даты, времени, подтверждение)
    if payload.startswith("ref_sel_"):
        await _handle_select_referral(bot, user_id, chat_id, ctx, cache, payload)
        return

    if payload.startswith("ref_doc_"):
        await _handle_select_doctor(bot, user_id, chat_id, ctx, cache, payload)
        return

    if payload.startswith("ref_date_"):
        await _handle_select_date(bot, user_id, chat_id, ctx, cache, payload)
        return

    if payload.startswith("ref_time_"):
        await _handle_select_time(bot, user_id, chat_id, ctx, cache, payload)
        return

    if payload == "ref_confirm_booking":
        await _handle_confirm_booking(bot, user_id, chat_id, ctx, cache)
        return


async def _show_other_patient_confirmation(bot, user_id: int, chat_id: int, ctx: ReferralOtherUserContext):
    summary_lines = [
        "ℹ️ Проверьте данные пациента для записи по направлению:\n",
        f"ФИО: {ctx.patient_fio or 'Не указано'}",
        f"Дата рождения: {ctx.patient_birthdate or 'Не указана'}",
        f"Полис ОМС: {ctx.patient_oms or 'Не указан'}",
    ]
    if ctx.patient_snils:
        summary_lines.append(f"СНИЛС: {ctx.patient_snils}")
    if ctx.patient_gender:
        summary_lines.append(f"Пол: {ctx.patient_gender}")
    text = "\n".join(summary_lines)

    keyboard = ref_kb_other.kb_confirm_other_patient()
    await bot.send_message(chat_id=chat_id, text=text, attachments=[keyboard] if keyboard else [])


async def _authorize_other_and_request_ref_number(
    bot, user_id: int, chat_id: int, ctx: ReferralOtherUserContext
):
    if not ctx.patient_fio or not ctx.patient_birthdate or not ctx.patient_oms:
        await bot.send_message(
            chat_id=chat_id,
            text="❌ Для записи по направлению нужны ФИО, дата рождения и полис ОМС пациента.",
        )
        return

    parts = ctx.patient_fio.split()
    if len(parts) < 3:
        parts = ["Иванов", "Иван", "Иванович"]

    birth_iso = ctx.patient_birthdate
    if "." in birth_iso:
        birth_iso = "-".join(birth_iso.split(".")[::-1])

    await bot.send_message(chat_id=chat_id, text="🔄 Авторизация в системе РМИС...")
    # Генерируем client_session_id как UUID (как в visit_a_doctor)
    ctx.client_session_id = str(uuid.uuid4())
    try:
        xml = await get_patient_session_other(
            oms=ctx.patient_oms,
            birthdate_iso=birth_iso,
            fio_parts=parts,
            client_session_id=ctx.client_session_id,
        )
    except Exception as e:
        await _soap_error(bot, user_id, chat_id, str(e))
        return

    session_id = parse_session_id(xml)
    if not session_id:
        log_data_event(user_id, "referral_other_auth_failed", xml=xml[:500])
        await bot.send_message(
            chat_id=chat_id,
            text="❌ Ошибка авторизации. Пациент не найден или данные некорректны.",
        )
        return

    ctx.session_id = session_id
    ctx.session_created_at = datetime.now()
    ctx.update_activity()
    log_data_event(user_id, "referral_other_auth_success", session_id=session_id)

    ctx.step = "OTHER_ENTER_REF_NUMBER"
    await bot.send_message(
        chat_id=chat_id,
        text="Введите номер направления (только цифры с бланка).",
        attachments=[ref_kb_other.kb_enter_referral_number_back()],
    )


async def handle_referral_other_text_input(bot, user_id: int, chat_id: int, text: str) -> bool:
    """Обработка текстового ввода для сценария другого по направлению."""
    if user_id not in other_states:
        return False
    ctx = other_states[user_id]
    ctx.update_activity()

    # Ввод ФИО
    if ctx.step == "OTHER_ENTER_FIO":
        if not re.match(r"^[А-ЯЁа-яё\-]+\s+[А-ЯЁа-яё\-]+\s+[А-ЯЁа-яё\-]+$", text):
            await bot.send_message(
                chat_id=chat_id,
                text="❌ Ошибка формата! Введите ФИО через пробел.\nПример: Иванова Анна Максимовна",
            )
            return True
        ctx.patient_fio = text.strip()
        ctx.step = "OTHER_ENTER_BIRTHDATE"
        await bot.send_message(chat_id=chat_id, text="Введите дату рождения пациента (ДД.ММ.ГГГГ):")
        return True

    # Ввод даты рождения
    if ctx.step == "OTHER_ENTER_BIRTHDATE":
        if not re.match(r"^\d{2}\.\d{2}\.\d{4}$", text.strip()):
            await bot.send_message(
                chat_id=chat_id,
                text="❌ Ошибка формата! Используйте ДД.ММ.ГГГГ",
            )
            return True
        ctx.patient_birthdate = text.strip()
        ctx.step = "OTHER_ENTER_OMS"
        await bot.send_message(
            chat_id=chat_id,
            text="Введите номер полиса ОМС пациента:",
        )
        return True

    # Ввод полиса ОМС
    if ctx.step == "OTHER_ENTER_OMS":
        oms = re.sub(r"[\s\-]", "", text).upper()
        if not re.fullmatch(r"[A-Z0-9]+", oms) or not (10 <= len(oms) <= 20):
            await bot.send_message(
                chat_id=chat_id,
                text="❌ Ошибка формата ОМС. Допустимы латинские буквы и цифры, длина 10–20 символов.",
            )
            return True
        ctx.patient_oms = oms
        ctx.step = "OTHER_CONFIRM_PATIENT"
        await _show_other_patient_confirmation(bot, user_id, chat_id, ctx)
        return True

    # Ввод номера направления
    if ctx.step == "OTHER_ENTER_REF_NUMBER":
        ref_number = (text or "").strip()
        if not ref_number:
            await bot.send_message(
                chat_id=chat_id,
                text="Введите номер направления (цифры с бланка).",
                attachments=[ref_kb_other.kb_enter_referral_number_back()],
            )
            return True

        ctx.referral_number = ref_number
        parts = ctx.patient_fio.split()
        if len(parts) < 3:
            parts = ["Иванов", "Иван", "Иванович"]
        birth_iso = ctx.patient_birthdate
        if "." in birth_iso:
            birth_iso = "-".join(birth_iso.split(".")[::-1])

        await bot.send_message(chat_id=chat_id, text="🔄 Поиск направления...")
        try:
            xml = await get_referral_info_other(
                session_id=ctx.session_id,
                oms=ctx.patient_oms,
                birthdate_iso=birth_iso,
                fio_parts=parts,
                referral_number=ref_number,
            )
        except Exception as e:
            await _soap_error(bot, user_id, chat_id, str(e))
            return True

        parsed = parse_get_referral_info_response(xml)
        if parsed.get("error_code"):
            code = parsed["error_code"]
            await bot.send_message(
                chat_id=chat_id,
                text="❌ Направление не найдено или недоступно. Проверьте номер и данные пациента.",
                attachments=[ref_kb_other.kb_enter_referral_number_back()],
            )

            if code == "SESSION_TIMED_OUT":
                other_states.pop(user_id, None)
                other_cache.pop(user_id, None)
            return True

        referrals = parsed.get("referrals", [])
        if not referrals:
            await bot.send_message(chat_id=chat_id, text=NO_SLOTS_MESSAGE)
            ctx.step = "OTHER_REF_LIST"
            ctx.referrals = []
            return True

        # Загрузка МО и подготовка списка направлений
        try:
            xml_mo = await get_mos(ctx.session_id)
            mos = parse_mo_list(xml_mo)
        except Exception:
            mos = []
        cache = get_cache(user_id)
        cache["mos"] = mos
        mo_by_oid = {m.get("oid"): m for m in mos if m.get("oid")}
        for r in referrals:
            num = r.get("referral_number", r.get("referral_id", ""))
            start = (r.get("referral_start_date") or "")[:10]
            if start:
                try:
                    dt_obj = datetime.strptime(start, "%Y-%m-%d")
                    start = dt_obj.strftime("%d.%m.%Y")
                except Exception:
                    pass
            text_disp = f"Направление №{num} от {start}"
            mo_oid = r.get("to_mo_oid", "")
            mo_name = (mo_by_oid.get(mo_oid) or {}).get("name", "")
            spec = ""
            if r.get("post_id"):
                spec = get_specialty_name(r["post_id"])
            elif r.get("specialty_id"):
                spec = get_specialty_name(r["specialty_id"])
            if spec:
                text_disp += f" ({spec})"
            if mo_name:
                text_disp += f" {mo_name}"
            r["display_text"] = text_disp

        ctx.referrals = referrals
        ctx.step = "OTHER_REF_LIST"
        ctx.ref_list_page = 0

        msg = "📄 Найдено направление. Выберите для записи:\n\n"
        for i, r in enumerate(referrals[:10], 1):
            msg += f"{i}. {r.get('display_text', r.get('referral_number', ''))}\n\n"

        from referral_visit import keyboards as ref_kb_base

        kb_list = ref_kb_base.kb_referral_list(referrals, 0)
        await bot.send_message(chat_id=chat_id, text=msg, attachments=[kb_list])
        return True

    return False


async def _handle_select_referral(bot, user_id: int, chat_id: int, ctx: ReferralOtherUserContext, cache, payload: str):
    ref_id = payload.replace("ref_sel_", "")
    selected = next((r for r in ctx.referrals if r.get("referral_id") == ref_id), None)
    if not selected:
        await bot.send_message(chat_id=chat_id, text="Направление не найдено.")
        return

    ctx.selected_referral = selected
    ctx.selected_referral_id = ref_id
    ctx.selected_referral_number = selected.get("referral_number", "")
    ctx.referral_start_date = (selected.get("referral_start_date") or "")[:10]
    ctx.referral_end_date = (selected.get("referral_end_date") or "")[:10]

    to_mo_oid = selected.get("to_mo_oid", "")
    mos = cache.get("mos", [])
    # Находим все подразделения, чей oid начинается с корневого OID
    subdivisions = [m for m in mos if m.get("oid", "").startswith(to_mo_oid)] if to_mo_oid else []

    if not subdivisions:
        # Падаем назад к исходному OID
        ctx.selected_mo_oid = to_mo_oid
        mo = next((m for m in mos if m.get("oid") == to_mo_oid), None)
        if mo:
            ctx.selected_mo_id = mo.get("id", "")
            ctx.selected_mo_name = mo.get("name", "")
        await _after_mo_selected_for_referral(bot, user_id, chat_id, ctx, cache)
        return

    if len(subdivisions) == 1:
        chosen = subdivisions[0]
        ctx.selected_mo_id = chosen.get("id", "")
        ctx.selected_mo_oid = chosen.get("oid", "")
        ctx.selected_mo_name = chosen.get("name", "")
        await _after_mo_selected_for_referral(bot, user_id, chat_id, ctx, cache)
        return

    # Несколько подразделений — предлагаем выбор в том же формате, что и при записи к врачу:
    # сначала нумерованный список МО (с названием и адресом), ниже — кнопки с номерами.
    cache["mo_subdivisions"] = subdivisions
    kb = ref_kb_other.kb_mo_subdivision_selection(subdivisions)

    menu_text = "🏥 Выберите подразделение медицинской организации по направлению:\n\n"
    for i, mo in enumerate(subdivisions):
        name = mo.get("name", "")
        address = mo.get("address", "")
        display = name
        if address:
            # Упрощённая очистка адреса: убираем лишние пробелы/запятые
            cleaned = re.sub(r",+", ",", address)
            cleaned = cleaned.strip(" ,")
            display = f"{name} ({cleaned})"
        menu_text += f"{i + 1}. {display}\n\n"

    await bot.send_message(
        chat_id=chat_id,
        text=menu_text,
        attachments=[kb] if kb else [],
    )


async def _after_mo_selected_for_referral(bot, user_id: int, chat_id: int, ctx: ReferralOtherUserContext, cache):
    # Логика похожа на referral_visit: если в направлении уже указан врач, идём сразу к датам, иначе — к выбору врача
    selected = ctx.selected_referral or {}
    ctx.selected_post_id = selected.get("post_id", "")
    ctx.selected_specialty_id = selected.get("specialty_id", "")
    ctx.selected_service_id = selected.get("service_id", "")

    to_snils = (selected.get("to_resource_snils") or "").strip()
    to_name = (selected.get("to_resource_name") or "").strip()

    if to_snils and to_name:
        ctx.selected_doctor_id = to_snils
        ctx.selected_doctor_name = to_name
        ctx.selected_resource_type = "specialist"
        if not ctx.selected_post_id:
            await bot.send_message(chat_id=chat_id, text=NO_SLOTS_MESSAGE)
            return
        start_d, end_d = _dates_from_referral(ctx)
        await bot.send_message(chat_id=chat_id, text="🔄 Поиск доступных дат...")
        try:
            xml_res = await get_doctors_referral(
                ctx.session_id,
                ctx.selected_post_id,
                ctx.selected_mo_oid,
                start_d,
                end_d,
            )
            doctors = parse_doctors(xml_res)
        except Exception as e:
            await _soap_error(bot, user_id, chat_id, str(e))
            return
        doc = next((d for d in doctors if d.get("id") == to_snils), None)
        if doc and doc.get("mo_oid"):
            ctx.selected_mo_oid = doc["mo_oid"]
        ctx.available_dates_cache = doc.get("dates", []) if doc else []
        if not ctx.available_dates_cache:
            await bot.send_message(chat_id=chat_id, text=NO_SLOTS_MESSAGE)
            return
        ctx.step = "OTHER_DATE"
        ctx.date_page = 0
        from referral_visit import keyboards as ref_kb_base

        keyboard = ref_kb_base.kb_date_selection(ctx.available_dates_cache, 0)
        await bot.send_message(
            chat_id=chat_id,
            text="Выберите дату приёма:",
            attachments=[keyboard],
        )
        return

    # Нет закреплённого врача — как в базовом сценарии: GetMOResourceInfo и выбор врача/кабинета
    if not ctx.selected_post_id:
        await bot.send_message(
            chat_id=chat_id,
            text="Запись по данному типу направления временно недоступна. Обратитесь в поликлинику.",
        )
        return

    start_d, end_d = _dates_from_referral(ctx)
    await bot.send_message(chat_id=chat_id, text="🔄 Поиск врачей и кабинетов...")
    try:
        xml_res = await get_doctors_referral(
            ctx.session_id,
            ctx.selected_post_id,
            ctx.selected_mo_oid,
            start_d,
            end_d,
        )
        doctors = parse_doctors(xml_res)
    except Exception as e:
        await _soap_error(bot, user_id, chat_id, str(e))
        return

    if not doctors:
        await bot.send_message(chat_id=chat_id, text=NO_SLOTS_MESSAGE)
        return
    cache["doctors"] = doctors
    ctx.selected_spec = get_specialty_name(ctx.selected_post_id)
    ctx.step = "OTHER_DOCTOR"

    from referral_visit import keyboards as ref_kb_base

    keyboard = ref_kb_base.kb_doctor_selection(doctors)
    await bot.send_message(
        chat_id=chat_id,
        text=f"Выберите врача или кабинет ({ctx.selected_spec}):",
        attachments=[keyboard],
    )


async def _handle_select_doctor(bot, user_id: int, chat_id: int, ctx: ReferralOtherUserContext, cache, payload: str):
    doc_id = payload.replace("ref_doc_", "")
    doctors = cache.get("doctors", [])
    found = next((d for d in doctors if d.get("id") == doc_id), None)
    if not found:
        return
    ctx.selected_doctor_id = doc_id
    ctx.selected_doctor_name = found.get("name", "")
    ctx.selected_resource_type = found.get("type", "specialist")
    ctx.available_dates_cache = found.get("dates", [])
    mo_oid_for_doc = found.get("mo_oid")
    if mo_oid_for_doc:
        ctx.selected_mo_oid = mo_oid_for_doc
    if found.get("type") == "room":
        ctx.selected_room_id = found.get("room_id", "")
        ctx.selected_room_oid = found.get("room_oid", "")
    if not ctx.available_dates_cache:
        await bot.send_message(chat_id=chat_id, text=NO_SLOTS_MESSAGE)
        return
    ctx.step = "OTHER_DATE"
    ctx.date_page = 0

    from referral_visit import keyboards as ref_kb_base

    keyboard = ref_kb_base.kb_date_selection(ctx.available_dates_cache, 0)
    await bot.send_message(chat_id=chat_id, text="Выберите дату приёма:", attachments=[keyboard])


async def _handle_select_date(bot, user_id: int, chat_id: int, ctx: ReferralOtherUserContext, cache, payload: str):
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
        await _soap_error(bot, user_id, chat_id, str(e))
        return
    if not slots:
        await bot.send_message(chat_id=chat_id, text=NO_SLOTS_MESSAGE)
        return
    cache["slots"] = slots
    ctx.available_slots_cache = slots
    ctx.step = "OTHER_TIME"
    ctx.time_page = 0

    from referral_visit import keyboards as ref_kb_base

    keyboard = ref_kb_base.kb_time_selection(slots, 0)
    await bot.send_message(
        chat_id=chat_id,
        text=f"Выберите время на {date_str}:",
        attachments=[keyboard],
    )


async def _handle_select_time(bot, user_id: int, chat_id: int, ctx: ReferralOtherUserContext, cache, payload: str):
    slot_id = payload.replace("ref_time_", "")
    slots = cache.get("slots", [])
    found = next((s for s in slots if s.get("id") == slot_id), None)
    if not found:
        return
    ctx.selected_slot_id = slot_id
    ctx.selected_time = found.get("time", "")
    ctx.selected_room = found.get("room", "")
    ctx.step = "OTHER_CONFIRM_BOOKING"

    from visit_a_doctor.specialties_MO import Abbreviations_MO

    mo_name = ctx.selected_mo_name
    try:
        mo_short = Abbreviations_MO.get(mo_name, mo_name)
    except Exception:
        mo_short = mo_name

    confirm_text = (
        f"ℹ️ Подтверждение записи по направлению №{ctx.selected_referral_number}\n\n"
        f"🏥 МО: {mo_short}\n"
        f"👨‍⚕️ Врач: {ctx.selected_doctor_name} ({ctx.selected_spec})\n"
        f"🚪 Кабинет: {ctx.selected_room}\n"
        f"🗓 Дата: {ctx.selected_date}\n"
        f"⏰ Время: {ctx.selected_time}\n\n"
        f"Пациент: {ctx.patient_fio}\n"
        "Подтвердить запись?"
    )

    from referral_visit import keyboards as ref_kb_base

    keyboard = ref_kb_base.kb_confirm_referral_booking()
    await bot.send_message(chat_id=chat_id, text=confirm_text, attachments=[keyboard])


async def _handle_confirm_booking(bot, user_id: int, chat_id: int, ctx: ReferralOtherUserContext, cache):
    await bot.send_message(chat_id=chat_id, text="🔄 Оформление записи...")
    try:
        xml = await book_appointment_referral(ctx.session_id, ctx.selected_slot_id)
        details = parse_create_appointment_details(xml)
        success = (details.get("status_code") or "").strip().upper() == "SUCCESS"
    except Exception as e:
        await _soap_error(bot, user_id, chat_id, str(e))
        return

    if success:
        visit_time_str = details.get("visit_time") or f"{ctx.selected_date} {ctx.selected_time}:00"
        room_str = details.get("room") or ctx.selected_room or ""
        book_id_mis = details.get("book_id_mis") or ""
        mo_address = ""
        mos = cache.get("mos", [])
        if mos and ctx.selected_mo_id:
            mo_obj = next((m for m in mos if m.get("id") == ctx.selected_mo_id), None)
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
        db.add_appointment(user_id, appointment_data, booking_source="referral_other_bot")

        summary = (
            "✅ Запись по направлению для другого пациента успешно оформлена!\n\n"
            f"Пациент: {ctx.patient_fio}\n"
            f"Направление №{ctx.selected_referral_number}\n"
            f"🏥 МО: {ctx.selected_mo_name}\n"
            f"👨‍⚕️ Врач: {ctx.selected_doctor_name}\n"
            f"🗓 Дата: {ctx.selected_date}\n"
            f"⏰ Время: {ctx.selected_time}\n\n"
            "За день до приёма придёт уведомление."
        )

        from referral_visit import keyboards as ref_kb_base

        await bot.send_message(chat_id=chat_id, text=summary, attachments=[ref_kb_base.kb_final_menu()])
    else:
        code = (details.get("status_code") or "").strip().upper()
        comment = (details.get("comment") or "").strip()
        if code == "APPOINT_PATIENT_REGISTERED_SPECIALIST":
            err = comment or "Пациент уже записан к этому специалисту."
        elif code == "APPOINT_TIME_IS_BUSY":
            err = "Время уже занято. Выберите другое."
        else:
            err = "Не удалось оформить запись. Попробуйте другое время."
        from referral_visit import keyboards as ref_kb_base

        await bot.send_message(
            chat_id=chat_id,
            text=f"❌ {err}",
            attachments=[ref_kb_base.kb_final_menu()],
        )

    other_states.pop(user_id, None)
    other_cache.pop(user_id, None)

