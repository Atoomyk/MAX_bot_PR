"""Клавиатуры для сценария записи по направлению ДРУГОГО человека."""
from bot_utils import create_keyboard


def get_back_button(payload: str) -> dict:
    return {"type": "callback", "text": "⬅️ Назад", "payload": payload}


def kb_person_choice_for_referral():
    """Экран выбора: записать себя / записать другого (для старта сценария направления)."""
    buttons = [
        [{"type": "callback", "text": "Записать себя по направлению", "payload": "ref_person_me"}],
        [{"type": "callback", "text": "Записать другого человека по направлению", "payload": "ref_person_other"}],
        [get_back_button("back_to_main")],
    ]
    return create_keyboard(buttons)


def kb_other_patient_candidates(patients: list[dict]):
    """Список прикреплённых пациентов + кнопка 'ввести вручную'."""
    rows = []
    for idx, p in enumerate(patients):
        fio = p.get("fio", "Без ФИО")
        birth = p.get("birth_date", "")
        text = f"{fio} ({birth})" if birth else fio
        rows.append(
            [
                {
                    "type": "callback",
                    "text": text,
                    "payload": f"ref_other_select_{idx}",
                }
            ]
        )
    rows.append(
        [
            {
                "type": "callback",
                "text": "➕ Ввести данные вручную",
                "payload": "ref_other_select_manual",
            }
        ]
    )
    rows.append([get_back_button("ref_restart")])
    return create_keyboard(rows)


def kb_confirm_other_patient():
    """Подтверждение данных другого пациента."""
    buttons = [
        [{"type": "callback", "text": "✅ Все верно, продолжить", "payload": "ref_other_confirm_patient"}],
        [{"type": "callback", "text": "✏️ Изменить данные пациента", "payload": "ref_other_edit_patient"}],
        [get_back_button("ref_restart")],
    ]
    return create_keyboard(buttons)


def kb_enter_referral_number_back():
    """Кнопка 'назад' при вводе номера направления."""
    buttons = [
        [get_back_button("ref_back_to_other_patient")],
    ]
    return create_keyboard(buttons)


def kb_mo_subdivision_selection(subdivisions: list[dict]):
    """
    Меню выбора подразделения МО по направлению.
    subdivisions: список словарей МО (id, name, oid, address).
    """
    if not subdivisions:
        return None
    rows: list[list[dict]] = []
    for idx, m in enumerate(subdivisions):
        name = m.get("name", "")
        addr = m.get("address", "")
        text = name
        if addr:
            text = f"{name} ({addr})"
        rows.append(
            [
                {
                    "type": "callback",
                    "text": str(idx + 1),
                    "payload": f"ref_other_mo_sub_{idx}",
                }
            ]
        )
    rows.append([get_back_button("ref_back_to_list")])
    return create_keyboard(rows)

