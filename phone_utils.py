import re


def normalize_phone_plus7(phone: str) -> str:
    """
    Приводит телефон к формату +7XXXXXXXXXX (best-effort).
    Предполагается, что в профиле телефон есть всегда.
    """
    if not phone:
        return ""

    digits = re.sub(r"\D", "", phone)

    # 10 цифр, начинается с 9 -> РФ мобильный
    if len(digits) == 10 and digits.startswith("9"):
        digits = "7" + digits

    # 11 цифр, начинается с 8 -> заменяем на 7
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]

    # 11 цифр, начинается с 7 -> ок
    if len(digits) == 11 and digits.startswith("7"):
        return "+{}".format(digits)

    # Фоллбек: всё равно вернём +<digits> (чтобы элемент Phone уходил всегда)
    return "+{}".format(digits) if digits else ""
