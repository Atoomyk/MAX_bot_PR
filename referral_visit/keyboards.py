# referral_visit/keyboards.py
"""Клавиатуры для сценария записи по направлению."""
from bot_utils import create_keyboard


def get_back_button(payload: str) -> dict:
    return {"type": "callback", "text": "⬅️ Назад", "payload": payload}


def kb_referral_list(referrals: list, page: int = 0, page_size: int = 5) -> object:
    """Список направлений с пагинацией. referrals — список dict с ключами display_text, ref_id для payload."""
    if not referrals:
        return None
    start = page * page_size
    end = min(start + page_size, len(referrals))
    chunk = referrals[start:end]
    buttons = []
    for r in chunk:
        ref_id = r.get("referral_id", r.get("ref_id", ""))
        text = r.get("display_text", f"Направление №{r.get('referral_number', ref_id)}")
        if len(text) > 50:
            text = text[:47] + "..."
        buttons.append([{"type": "callback", "text": text, "payload": f"ref_sel_{ref_id}"}])
    nav = []
    if page > 0:
        nav.append({"type": "callback", "text": "⬅️ Пред.", "payload": f"ref_list_page_{page - 1}"})
    if end < len(referrals):
        nav.append({"type": "callback", "text": "След. ➡️", "payload": f"ref_list_page_{page + 1}"})
    if nav:
        buttons.append(nav)
    buttons.append([{"type": "callback", "text": "🔍 Ввести номер направления", "payload": "ref_find_by_number"}])
    buttons.append([get_back_button("back_to_main")])
    return create_keyboard(buttons)


def kb_no_referrals_find() -> object:
    """Когда направлений нет: сообщение + кнопка «Найти направление»."""
    return create_keyboard([
        [{"type": "callback", "text": "🔍 Найти направление", "payload": "ref_find_by_number"}],
        [get_back_button("back_to_main")],
    ])


def kb_enter_referral_number_back() -> object:
    """Назад при вводе номера направления."""
    return create_keyboard([[get_back_button("ref_back_to_list")]])


def kb_doctor_selection(doctors: list) -> object:
    """Выбор врача/кабинета (как в visit_a_doctor, но payload ref_doc_)."""
    if not doctors:
        return None
    from bot_utils import create_keyboard
    buttons = []
    for d in doctors:
        buttons.append([{"type": "callback", "text": d.get("name", d.get("id", "")), "payload": f"ref_doc_{d['id']}"}])
    buttons.append([get_back_button("ref_back_to_list")])
    return create_keyboard(buttons)


def kb_date_selection(dates: list, page: int = 0, page_size: int = 6) -> object:
    """Выбор даты. dates — список строк DD.MM.YYYY."""
    if not dates:
        return None
    from bot_utils import create_keyboard
    start = page * page_size
    end = min(start + page_size, len(dates))
    chunk = dates[start:end]
    buttons = []
    for d in chunk:
        buttons.append([{"type": "callback", "text": d, "payload": f"ref_date_{d}"}])
    nav = []
    if page > 0:
        nav.append({"type": "callback", "text": "⬅️ Пред.", "payload": f"ref_date_page_{page - 1}"})
    if end < len(dates):
        nav.append({"type": "callback", "text": "След. ➡️", "payload": f"ref_date_page_{page + 1}"})
    if nav:
        buttons.append(nav)
    buttons.append([get_back_button("ref_back_to_doc")])
    return create_keyboard(buttons)


def kb_time_selection(slots: list, page: int = 0, page_size: int = 10) -> object:
    """Выбор времени. slots — list[dict] с id, time, room."""
    if not slots:
        return None
    from bot_utils import create_keyboard
    start = page * page_size
    end = min(start + page_size, len(slots))
    chunk = slots[start:end]
    buttons = []
    for s in chunk:
        buttons.append([{"type": "callback", "text": s.get("time", s.get("id", "")), "payload": f"ref_time_{s['id']}"}])
    nav = []
    if page > 0:
        nav.append({"type": "callback", "text": "⬅️ Пред.", "payload": f"ref_time_page_{page - 1}"})
    if end < len(slots):
        nav.append({"type": "callback", "text": "След. ➡️", "payload": f"ref_time_page_{page + 1}"})
    if nav:
        buttons.append(nav)
    buttons.append([get_back_button("ref_back_to_date")])
    return create_keyboard(buttons)


def kb_confirm_referral_booking() -> object:
    """Подтверждение записи по направлению."""
    return create_keyboard([
        [{"type": "callback", "text": "✅ Подтвердить запись", "payload": "ref_confirm_booking"}],
        [get_back_button("ref_back_to_time")],
    ])


def kb_final_menu() -> object:
    """После успешной записи."""
    return create_keyboard([[{"type": "callback", "text": "🏠 Главное меню", "payload": "back_to_main"}]])


def kb_restart() -> object:
    """Начать сначала."""
    return create_keyboard([[{"type": "callback", "text": "🔄 Начать сначала", "payload": "ref_restart"}]])
