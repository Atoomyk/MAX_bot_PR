import re
import asyncio
from datetime import datetime
from typing import Dict, Any, Callable, Optional
from maxapi import Bot
from maxapi.types import InputMedia

from user_database import db
from logging_config import log_user_event, log_data_event, log_system_event
from bot_utils import create_keyboard
from patient_api_client import get_patients_by_phone
from esia import (
    generate_esia_url,
    wait_for_esia_file,
    parse_esia_file,
    save_esia_data_to_db,
    delete_esia_file
)
import asyncio

# Callback-константы для регистрации
# SOGL_LINK = "https://sevmiac.ru/upload/iblock/d73/sttjnvlhg3j2df943ve0fv3husrlm8oj.pdf"
CONTINUE_CALLBACK = "start_continue"
AGREEMENT_CALLBACK = "agreement_accepted"
ADMIN_CONTACT = "@admin_MIAC"

CORRECT_FIO_CALLBACK = "correct_fio"
CORRECT_BIRTH_DATE_CALLBACK = "correct_birth_date"
CORRECT_SNILS_CALLBACK = "correct_snils"
CORRECT_OMS_CALLBACK = "correct_oms"
CORRECT_GENDER_CALLBACK = "correct_gender"
CORRECT_PHONE_CALLBACK = "correct_phone"
CONFIRM_DATA_CALLBACK = "confirm_data"
CONFIRM_PHONE_CALLBACK = "confirm_phone"
REJECT_PHONE_CALLBACK = "reject_phone"

GENDER_MALE_CALLBACK = "gender_male"
GENDER_FEMALE_CALLBACK = "gender_female"


class RegistrationHandler:
    """Обработчик процесса регистрации пользователя"""

    def __init__(self, user_states: Dict[str, Any]):
        self.user_states = user_states

    async def send_agreement_message(self, bot_instance: Bot, user_id: int, chat_id: int):
        """Отправляет сообщение с соглашением"""
        keyboard = create_keyboard([[
            {'type': 'callback', 'text': 'Согласие на обработку персональных данных', 'payload': AGREEMENT_CALLBACK}
        ]])

        import os
        consent_file_path = os.path.join(os.getcwd(), "assets", "Soglasie.txt")
        
        attachments = []
        if os.path.exists(consent_file_path):
             attachments.append(InputMedia(path=consent_file_path))
        
        if keyboard:
            attachments.append(keyboard)

        await bot_instance.send_message(
            chat_id=chat_id,
            text='Продолжая, Вы даёте согласие на обработку персональных данных.\nПолный текст документа прикреплен к этому сообщению 👇',
            attachments=attachments
        )

    async def start_registration_process(self, bot_instance: Bot, user_id: int, chat_id: int):
        """Начинает процесс регистрации - подтверждение телефона"""
        self.user_states[user_id] = {'state': 'waiting_phone_confirmation', 'data': {}}
        log_user_event(user_id, "registration_started")

        await bot_instance.send_message(
            chat_id=chat_id,
            text='Для начала работы необходимо подтвердить номер и пройти регистрацию.'
        )
        await self.request_contact(bot_instance, chat_id)

    async def request_contact(self, bot_instance: Bot, chat_id: int):
        """Запрашивает контакт пользователя"""
        keyboard = create_keyboard([[
            {'type': 'contact', 'text': '📇 Отправить контакт'}
        ]])

        await bot_instance.send_message(
            chat_id=chat_id,
            text="Нажмите кнопку ниже чтобы поделиться контактом:",
            attachments=[keyboard] if keyboard else []
        )

    async def send_phone_confirmation(self, bot_instance: Bot, chat_id: int, phone: str):
        """Отправляет сообщение с подтверждением номера телефона"""
        keyboard = create_keyboard([[
            {'type': 'callback', 'text': '✅ Да, номер верный', 'payload': CONFIRM_PHONE_CALLBACK},
            {'type': 'callback', 'text': '❌ Нет, неверный номер', 'payload': REJECT_PHONE_CALLBACK}
        ]])

        await bot_instance.send_message(
            chat_id=chat_id,
            text=f"📞 Ваш номер телефона определён:\n\n📱 {phone}\n\nПожалуйста, проверьте актуальность номера:",
            attachments=[keyboard] if keyboard else []
        )

    async def handle_incorrect_phone(self, bot_instance: Bot, user_id: int, chat_id: int):
        """Обработка неверного номера телефона"""
        log_user_event(user_id, "phone_rejected")
        await bot_instance.send_message(
            chat_id=chat_id,
            text="❌ Пожалуйста, отправьте контакт с правильным номером телефона."
        )
        await self.request_contact(bot_instance, chat_id)

    async def start_fio_request(self, bot_instance: Bot, user_id: int, chat_id: int, user_data: dict):
        """Начинает процесс ввода ФИО"""
        self.user_states[user_id] = {'state': 'waiting_fio', 'data': user_data}
        log_user_event(user_id, "fio_input_started")

        await bot_instance.send_message(
            chat_id=chat_id,
            text='Пожалуйста, введите ваше ФИО в формате:\nФамилия Имя Отчество\n\nПример: Иванов Иван Иванович'
        )

    async def request_birth_date(self, bot_instance: Bot, user_id: int, chat_id: int, user_data: dict):
        """Запрашивает дату рождения"""
        self.user_states[user_id] = {'state': 'waiting_birth_date', 'data': user_data}

        await bot_instance.send_message(
            chat_id=chat_id,
            text="Отлично!\nТеперь введите вашу дату рождения\n\nФормат: ДД.ММ.ГГГГ\nПример: 13.03.2003"
        )

    async def request_snils(self, bot_instance: Bot, user_id: int, chat_id: int, user_data: dict):
        """Запрашивает СНИЛС"""
        self.user_states[user_id] = {'state': 'waiting_snils', 'data': user_data}
        await bot_instance.send_message(
            chat_id=chat_id,
            text="Теперь введите ваш СНИЛС (11 цифр).\nМожно с дефисами и пробелами."
        )

    async def request_oms(self, bot_instance: Bot, user_id: int, chat_id: int, user_data: dict):
        """Запрашивает полис ОМС"""
        self.user_states[user_id] = {'state': 'waiting_oms', 'data': user_data}
        await bot_instance.send_message(
            chat_id=chat_id,
            text="Введите номер полиса ОМС (16 цифр)."
        )

    async def request_gender(self, bot_instance: Bot, user_id: int, chat_id: int, user_data: dict):
        """Запрашивает пол пользователя"""
        current_state = self.user_states.get(user_id, {})
        new_state = {'state': 'waiting_gender', 'data': user_data}
        if 'candidates' in current_state:
            new_state['candidates'] = current_state['candidates']
            
        self.user_states[user_id] = new_state
        
        keyboard = create_keyboard([[
            {'type': 'callback', 'text': 'Мужской', 'payload': GENDER_MALE_CALLBACK},
            {'type': 'callback', 'text': 'Женский', 'payload': GENDER_FEMALE_CALLBACK}
        ]])
        await bot_instance.send_message(
            chat_id=chat_id,
            text="Выберите ваш пол:",
            attachments=[keyboard]
        )

    async def send_confirmation_message(self, bot_instance: Bot, user_id: int, chat_id: int, user_data: dict):
        """Отправляет сообщение с подтверждением данных"""
        fio = user_data.get('fio', 'Не указано')
        birth_date = user_data.get('birth_date', 'Не указано')
        phone = user_data.get('phone', 'Не указано')
        snils = user_data.get('snils', 'Не указано')
        oms = user_data.get('oms', 'Не указано')
        gender = user_data.get('gender', 'Не указано')
        
        is_from_rms = user_data.get('is_from_rms', False)

        log_data_event(user_id, "confirmation_prepared", fio=fio, birth_date=birth_date, phone=phone, is_from_rms=is_from_rms)
        
        # Кнопки редактирования показываем только если данные НЕ из РМИС
        buttons_config = []
        
        if not is_from_rms:
            buttons_config.extend([
                [{'type': 'callback', 'text': '⚠️ Исправить ФИО', 'payload': CORRECT_FIO_CALLBACK}],
                [{'type': 'callback', 'text': '⚠️ Исправить дату рождения', 'payload': CORRECT_BIRTH_DATE_CALLBACK}],
                [{'type': 'callback', 'text': '⚠️ Исправить СНИЛС', 'payload': CORRECT_SNILS_CALLBACK}],
                [{'type': 'callback', 'text': '⚠️ Исправить ОМС', 'payload': CORRECT_OMS_CALLBACK}],
                [{'type': 'callback', 'text': '⚠️ Исправить пол', 'payload': CORRECT_GENDER_CALLBACK}]
            ])
        else:
            # Данные из РМИС - редактирование запрещено, но можно сообщить об ошибке
            buttons_config.append([{'type': 'callback', 'text': '❌ Нашли ошибку?', 'payload': "reg_incorrect_data"}])
            
        buttons_config.append([{'type': 'callback', 'text': '✅ Всё верно, подтвердить', 'payload': CONFIRM_DATA_CALLBACK}])

        # Если есть список кандидатов, добавляем кнопку "Назад"
        if self.user_states.get(user_id, {}).get('candidates'):
             buttons_config.append([{'type': 'callback', 'text': '🔙 Назад к выбору', 'payload': 'reg_back_to_list'}])

        keyboard = create_keyboard(buttons_config)
        
        edit_hint = "" if is_from_rms else "\nЕсли всё верно - нажмите 'Подтвердить', или выберите что нужно исправить:"

        await bot_instance.send_message(
            chat_id=chat_id,
            text=f"📋 Пожалуйста, проверьте личные данные:\n\n👤 ФИО: {fio}\n🎂 Дата рождения: {birth_date}\n📞 Телефон: {phone}\n💳 СНИЛС: {snils}\n🏥 ОМС: {oms}\n⚧ Пол: {gender}{edit_hint}",
            attachments=[keyboard] if keyboard else []
        )

    async def complete_registration(self, bot_instance: Bot, user_id: int, chat_id: int, user_data: dict):
        """Завершает регистрацию и показывает главное меню"""
        fio = user_data['fio']
        birth_date = user_data['birth_date']
        phone = user_data['phone']
        snils = user_data.get('snils')
        oms = user_data.get('oms')
        gender = user_data.get('gender')

        try:
            async with asyncio.timeout(10):
                success = db.register_user(user_id, chat_id, fio, phone, birth_date, snils, oms, gender)
        except asyncio.TimeoutError:
            log_system_event("db_timeout", user_id=user_id)
            await bot_instance.send_message(
                chat_id=chat_id,
                text="⏳ Сервер перегружен, попробуйте позже"
            )
            return

        if success:
            self.user_states.pop(user_id, None)
            greeting_name = db.get_user_greeting(user_id)
            log_data_event(user_id, "registration_completed", fio=fio, phone=phone, status="success")

            await bot_instance.send_message(
                chat_id=chat_id,
                text="✅ Успешная регистрация!\nТеперь вы можете пользоваться всеми функциями бота."
            )
            return greeting_name
        else:
            self.user_states.pop(user_id, None)
            log_data_event(user_id, "registration_failed", fio=fio, phone=phone, status="duplicate")
            await bot_instance.send_message(
                chat_id=chat_id,
                text=f"🚨 Ошибка при регистрации. Комбинация ФИО и телефона уже существует.\n\nПожалуйста, обратитесь к администратору, {ADMIN_CONTACT}."
            )
            return None

    async def validate_and_process_input(self, user_id: int, input_text: str, input_type: str,
                                         bot_instance: Bot, chat_id: int, user_data: dict):
        """Универсальная функция валидации и обработки ввода для регистрации"""
        validator_map = {
            'fio': db.validate_fio,
            'birth_date': db.validate_birth_date,
            'phone': db.validate_phone,
            'snils': db.validate_snils,
            'oms': db.validate_oms,
            'gender': db.validate_gender
        }

        error_messages = {
            'fio': "❌ Ошибка формата!\n\nПожалуйста, введите ваше ФИО в формате: Фамилия Имя Отчество\n\nПример: Иванов Иван Иванович",
            'birth_date': "❌ Ошибка формата!\n\nПожалуйста, введите дату рождения в формате: ДД.ММ.ГГГГ\n\nПример: 13.03.2003",
            'phone': "❌ Неверный формат номера телефона.",
            'snils': "❌ Неверный формат СНИЛС (нужно 11 цифр).",
            'oms': "❌ Неверный формат ОМС (нужно 16 цифр).",
            'gender': "❌ Неверный формат пола."
        }

        if input_type not in validator_map:
            return False

        if not validator_map[input_type](input_text):
            log_user_event(user_id, f"invalid_{input_type}_format", input=input_text)
            await bot_instance.send_message(chat_id=chat_id, text=error_messages[input_type])
            return False

        # Сохраняем данные
        user_data[input_type] = input_text
        log_data_event(user_id, f"{input_type}_entered", **{input_type: input_text})
        return True

    async def request_data_correction(self, bot_instance: Bot, user_id: int, chat_id: int, user_data: dict, data_type: str):
        """Универсальная функция запроса исправления данных"""
        correction_configs = {
            'fio': {
                'state': 'waiting_fio_correction',
                'log_event': 'fio_correction_requested',
                'message': "Введите ваше ФИО для исправления:\n\nФормат: Фамилия Имя Отчество\nПример: Иванов Иван Иванович"
            },
            'birth_date': {
                'state': 'waiting_birth_date_correction',
                'log_event': 'birth_date_correction_requested',
                'message': "Введите вашу дату рождения для исправления:\n\nФормат: ДД.ММ.ГГГГ\nПример: 13.03.2003"
            },
            'snils': {
                'state': 'waiting_snils_correction',
                'log_event': 'snils_correction_requested',
                'message': "Введите ваш СНИЛС для исправления (11 цифр)."
            },
            'oms': {
                'state': 'waiting_oms_correction',
                'log_event': 'oms_correction_requested',
                'message': "Введите ваш полис ОМС для исправления (16 цифр)."
            },
            'gender': {
                'state': 'waiting_gender_correction',
                'log_event': 'gender_correction_requested',
                'message': "Выберите ваш пол для исправления:"
            }
        }

        if data_type not in correction_configs:
            return

        config = correction_configs[data_type]
        self.user_states[user_id] = {'state': config['state'], 'data': user_data}
        log_user_event(user_id, config['log_event'])

        attachments = []
        if data_type == 'gender':
            keyboard = create_keyboard([[
                {'type': 'callback', 'text': 'Мужской', 'payload': GENDER_MALE_CALLBACK},
                {'type': 'callback', 'text': 'Женский', 'payload': GENDER_FEMALE_CALLBACK}
            ]])
            if keyboard:
                attachments.append(keyboard)

        await bot_instance.send_message(chat_id=chat_id, text=config['message'], attachments=attachments)

    def _is_adult(self, birth_date_str: str) -> bool:
        """Проверяет, есть ли пользователю 18 лет. Формат: ДД.ММ.ГГГГ"""
        try:
            birth_date = datetime.strptime(birth_date_str, "%d.%m.%Y")
            today = datetime.now()
            age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
            return age >= 18
        except ValueError:
            return False

    async def handle_phone_confirmation(self, bot_instance: Bot, user_id: int, chat_id: int):
        """Обработка подтверждения телефона"""
        log_user_event(user_id, "phone_confirmed")
        current_state = self.user_states.get(user_id, {})
        user_data = current_state.get('data', {})

        if 'phone' not in user_data:
            log_data_event(user_id, "phone_missing_on_confirmation")
            await bot_instance.send_message(chat_id=chat_id,
                                            text="❌ Ошибка: номер телефона не найден. Начинаем регистрацию заново.")
            await self.start_registration_process(bot_instance, user_id, chat_id)
            return

        # ⚡ ЗАПРОС К API ПАЦИЕНТОВ ⚡
        await bot_instance.send_message(chat_id=chat_id, text="🔄 Проверяем данные в Рег. системе...")
        found_patients = await get_patients_by_phone(user_data['phone'])
        
        # Фильтрация несовершеннолетних
        adult_patients = [p for p in found_patients if self._is_adult(p.get('birth_date', ''))]
        
        if not adult_patients:
            # Если ничего не нашли (или все несовершеннолетние) — предлагаем ЕСИА
            await self.show_esia_option(bot_instance, user_id, chat_id, user_data)
            return

        # Если нашли ровно одного взрослого — выбираем автоматически
        if len(adult_patients) == 1:
            p = adult_patients[0]
            user_data['fio'] = p['fio']
            user_data['birth_date'] = p['birth_date']
            user_data['snils'] = p['snils']
            user_data['oms'] = p['oms']
            user_data['gender'] = p.get('gender')
            user_data['is_from_rms'] = True  # Флаг: данные из РМИС

            # Устанавливаем стейт (без candidates, т.к. выбор был безальтернативный)
            # Если пол есть - сразу к подтверждению, иначе запрашиваем
            if user_data.get('gender'):
                 self.user_states[user_id] = {'state': 'waiting_confirmation', 'data': user_data}
                 log_data_event(user_id, "identity_autoselected_single", snils=user_data['snils'], gender_autofilled=True)
                 await self.send_confirmation_message(bot_instance, user_id, chat_id, user_data)
            else:
                 self.user_states[user_id] = {'state': 'waiting_gender', 'data': user_data}
                 log_data_event(user_id, "identity_autoselected_single", snils=user_data['snils'])
                 await self.request_gender(bot_instance, user_id, chat_id, user_data)
            return

        # Если нашли взрослых (>1) — предлагаем выбрать
        self.user_states[user_id] = {'state': 'waiting_identity_selection', 'data': user_data, 'candidates': adult_patients}
        
        keyboard_rows = []
        for idx, p in enumerate(adult_patients):
            btn_text = f"{p['fio']} ({p['birth_date']})"
            keyboard_rows.append([{'type': 'callback', 'text': btn_text, 'payload': f"reg_identity_{idx}"}])
        
        # Ручной ввод убран по требованию
        
        keyboard = create_keyboard(keyboard_rows)
        
        await bot_instance.send_message(
            chat_id=chat_id,
            #text="🔍 По вашему номеру найдены следующие пациенты.\nКто вы?",
            text="🔍 На ваш номер зарегистрировано несколько медицинских карт.\nЧтобы пройти регистрацию выберете себя!",
            attachments=[keyboard]
        )

    async def handle_data_correction(self, bot_instance: Bot, user_id: int, chat_id: int, data_type: str):
        """Обработка исправления данных"""
        current_data = self.user_states.get(user_id, {}).get('data', {})
        current_data.pop(data_type, None)
        await self.request_data_correction(bot_instance, user_id, chat_id, current_data, data_type)

    async def handle_identity_selection(self, bot_instance: Bot, user_id: int, chat_id: int, selection_idx: str):
        """Обработка выбора личности из списка API"""
        current_state = self.user_states.get(user_id, {})
        user_data = current_state.get('data', {})
        candidates = current_state.get('candidates', [])

        if selection_idx == 'manual':
            user_data['is_from_rms'] = False
            await self.start_fio_request(bot_instance, user_id, chat_id, user_data)
            return

        try:
            idx = int(selection_idx)
            selected_patient = candidates[idx]
        except (ValueError, IndexError):
            await bot_instance.send_message(chat_id=chat_id, text="⚠ Ошибка выбора. Пробуем вручную.")
            user_data['is_from_rms'] = False
            await self.start_fio_request(bot_instance, user_id, chat_id, user_data)
            return

        # Автозаполнение данных
        user_data['fio'] = selected_patient['fio']
        user_data['birth_date'] = selected_patient['birth_date']
        user_data['snils'] = selected_patient['snils']
        user_data['oms'] = selected_patient['oms']
        user_data['gender'] = selected_patient.get('gender') 
        user_data['is_from_rms'] = True # Флаг: данные из РМИС

        if user_data.get('gender'):
            # Пол есть - сразу к подтверждению
            self.user_states[user_id] = {
                'state': 'waiting_confirmation',
                'data': user_data,
                'candidates': candidates 
            }
            log_data_event(user_id, "identity_autofilled", snils=user_data['snils'], gender_autofilled=True)
            await self.send_confirmation_message(bot_instance, user_id, chat_id, user_data)
        else:
             # Пола нет - запрашиваем
            self.user_states[user_id] = {
                'state': 'waiting_gender',
                'data': user_data,
                'candidates': candidates 
            }
            log_data_event(user_id, "identity_autofilled", snils=user_data['snils'])
            await self.request_gender(bot_instance, user_id, chat_id, user_data)
 
    async def handle_back_to_list(self, bot_instance: Bot, user_id: int, chat_id: int):
        """Возврат к экрану выбора личности"""
        current_state = self.user_states.get(user_id, {})
        candidates = current_state.get('candidates', [])
        user_data = current_state.get('data', {})

        if not candidates:
            # Если кандидатов нет в стейте, значит что-то пошло не так
            await self.start_registration_process(bot_instance, user_id, chat_id)
            return

        self.user_states[user_id] = {'state': 'waiting_identity_selection', 'data': user_data, 'candidates': candidates}

        keyboard_rows = []
        for idx, p in enumerate(candidates):
            btn_text = f"{p['fio']} ({p['birth_date']})"
            keyboard_rows.append([{'type': 'callback', 'text': btn_text, 'payload': f"reg_identity_{idx}"}])
        
        # Ручной ввод убран по требованию
        
        keyboard = create_keyboard(keyboard_rows)

        await bot_instance.send_message(
            chat_id=chat_id,
            text="🔍 На ваш номер зарегистрировано несколько медицинских карт.\nЧтобы пройти регистрацию выберете себя!",
            attachments=[keyboard]
        )

    async def handle_data_confirmation(self, bot_instance: Bot, user_id: int, chat_id: int):
        """Обработка подтверждения данных"""
        log_user_event(user_id, "user_confirmed_registration")
        user_data = self.user_states.get(user_id, {}).get('data', {})

        if user_data and all(key in user_data for key in ['fio', 'birth_date', 'phone', 'snils', 'oms', 'gender']):
            return await self.complete_registration(bot_instance, user_id, chat_id, user_data)
        else:
            missing_fields = [key for key in ['fio', 'birth_date', 'phone', 'snils', 'oms', 'gender'] if key not in user_data]
            log_data_event(user_id, "incomplete_data_on_confirmation", missing=missing_fields)
            await bot_instance.send_message(chat_id=chat_id,
                                            text="❌ Не все данные заполнены. Начинаем регистрацию заново.")
            await self.start_registration_process(bot_instance, user_id, chat_id)
            return None

    async def process_contact_message(self, event, user_id: int, chat_id: int):
        """Обработка сообщений с контактами для регистрации"""
        state_info = self.user_states.get(user_id)
        if not state_info or state_info.get('state') != 'waiting_phone_confirmation':
            return False

        contact_attachments = [attr for attr in event.message.body.attachments if attr.type == "contact"]
        if not contact_attachments:
            return False

        for contact in contact_attachments:
            try:
                payload = contact.payload
                vcf_info = payload.vcf_info
                phone_match = re.search(r'TEL[^:]*:([^\r\n]+)', vcf_info)

                if phone_match:
                    phone = phone_match.group(1).strip()
                    clean_phone = re.sub(r'[^\d+]', '', phone)
                    if not clean_phone.startswith('+'):
                        clean_phone = '+' + clean_phone
                    if not db.validate_phone(clean_phone):
                        log_user_event(user_id, "invalid_phone_format", phone=clean_phone)
                        await event.bot.send_message(chat_id=chat_id, text="❌ Неверный формат номера телефона.")
                        return True

                    user_data = state_info.get('data', {})
                    user_data['phone'] = clean_phone
                    self.user_states[user_id] = {'state': 'waiting_phone_confirmation', 'data': user_data}

                    log_data_event(user_id, "phone_extracted", phone=clean_phone)
                    await self.send_phone_confirmation(event.bot, chat_id, clean_phone)
                    return True
                else:
                    log_user_event(user_id, "phone_extraction_failed")
                    await event.bot.send_message(chat_id=chat_id, text="❌ Не удалось определить номер телефона.")
                    return True

            except Exception as e:
                log_system_event("contact_handler", "processing_failed", error=str(e), user_id=user_id)
                await event.bot.send_message(chat_id=chat_id, text="❌ Произошла ошибка при обработке контакта.")
                return True

        return False

    async def process_text_input(self, user_id: int, message_text: str, bot_instance: Bot, chat_id: int):
        """Обработка текстового ввода в процессе регистрации"""
        state_info = self.user_states.get(user_id)
        if not state_info:
            return False

        state = state_info.get('state')
        user_data = state_info.get('data', {})

        # Обработка разных состояний регистрации
        state_handlers = {
            'waiting_fio': lambda: self._handle_fio_input(user_id, message_text, bot_instance, chat_id, user_data),
            'waiting_birth_date': lambda: self._handle_birth_date_input(user_id, message_text, bot_instance,
                                                                        chat_id, user_data),
            'waiting_snils': lambda: self._handle_snils_input(user_id, message_text, bot_instance, chat_id, user_data),
            'waiting_oms': lambda: self._handle_oms_input(user_id, message_text, bot_instance, chat_id, user_data),
            'waiting_gender': lambda: self._handle_gender_input(user_id, message_text, bot_instance, chat_id, user_data),
            'waiting_fio_correction': lambda: self._handle_fio_correction(user_id, message_text, bot_instance,
                                                                          chat_id, user_data),
            'waiting_birth_date_correction': lambda: self._handle_birth_date_correction(user_id, message_text,
                                                                                        bot_instance, chat_id,
                                                                                        user_data),
            'waiting_snils_correction': lambda: self._handle_snils_correction(user_id, message_text, bot_instance,
                                                                              chat_id, user_data),
            'waiting_oms_correction': lambda: self._handle_oms_correction(user_id, message_text, bot_instance,
                                                                          chat_id, user_data),
            'waiting_gender_correction': lambda: self._handle_gender_correction(user_id, message_text, bot_instance,
                                                                                chat_id, user_data),
        }

        if state in state_handlers:
            result = await state_handlers[state]()
            return result is not False  # Возвращаем True если состояние обработано

        return False

    async def _handle_fio_input(self, user_id: int, message_text: str, bot_instance: Bot, chat_id: int,
                                user_data: dict):
        """Обработка ввода ФИО"""
        success = await self.validate_and_process_input(user_id, message_text, 'fio', bot_instance, chat_id,
                                                        user_data)
        if success:
            await self.request_birth_date(bot_instance, user_id, chat_id, user_data)
        return success

    async def _handle_snils_input(self, user_id: int, message_text: str, bot_instance: Bot, chat_id: int,
                                  user_data: dict):
        """Обработка ввода СНИЛС"""
        success = await self.validate_and_process_input(user_id, message_text, 'snils', bot_instance, chat_id,
                                                        user_data)
        if success:
            await self.request_oms(bot_instance, user_id, chat_id, user_data)
        return success

    async def _handle_oms_input(self, user_id: int, message_text: str, bot_instance: Bot, chat_id: int,
                                user_data: dict):
        """Обработка ввода ОМС"""
        success = await self.validate_and_process_input(user_id, message_text, 'oms', bot_instance, chat_id,
                                                        user_data)
        if success:
            await self.request_gender(bot_instance, user_id, chat_id, user_data)
        return success

    async def _handle_gender_input(self, user_id: int, message_text: str, bot_instance: Bot, chat_id: int,
                                   user_data: dict):
        """Обработка ввода пола (текст)"""
        success = await self.validate_and_process_input(user_id, message_text, 'gender', bot_instance, chat_id,
                                                        user_data)
        if success:
            current_state = self.user_states.get(user_id, {})
            new_state = {'state': 'waiting_confirmation', 'data': user_data}
            if 'candidates' in current_state:
                new_state['candidates'] = current_state['candidates']

            self.user_states[user_id] = new_state
            await self.send_confirmation_message(bot_instance, user_id, chat_id, user_data)
        return success

    async def handle_gender_choice(self, bot_instance: Bot, user_id: int, chat_id: int, gender: str):
        """Обработка выбора пола через кнопки"""
        current_state = self.user_states.get(user_id, {})
        user_data = current_state.get('data', {})
        
        # Валидация и сохранение
        if gender not in ["Мужской", "Женский"]:
            return False
            
        user_data['gender'] = gender
        log_data_event(user_id, "gender_selected", gender=gender)
        
        # Если это была коррекция — возвращаемся к подтверждению
        if current_state.get('state') == 'waiting_gender_correction':
             self.user_states[user_id] = {'state': 'waiting_confirmation', 'data': user_data}
             await self.send_confirmation_message(bot_instance, user_id, chat_id, user_data)
             return True
             
        # Если обычный флоу регистрации — переходим к подтверждению
        next_state = {
            'state': 'waiting_confirmation',
            'data': user_data,
        }
        # Пробрасываем кандидатов, если они были
        if 'candidates' in current_state:
            next_state['candidates'] = current_state['candidates']

        self.user_states[user_id] = next_state
        await self.send_confirmation_message(bot_instance, user_id, chat_id, user_data)
        return True

    async def _handle_birth_date_input(self, user_id: int, message_text: str, bot_instance: Bot, chat_id: int,
                                       user_data: dict):
        """Обработка ввода даты рождения"""
        success = await self.validate_and_process_input(user_id, message_text, 'birth_date', bot_instance, chat_id,
                                                        user_data)
        if success:
            # Дополнительная проверка на 18+
            if not self._is_adult(message_text):
                await bot_instance.send_message(chat_id=chat_id, 
                                                text="⛔ Регистрация доступна только для пользователей старше 18 лет.")
                # Оставляем в текущем состоянии, чтобы мог исправить или отменить
                return True

            await self.request_snils(bot_instance, user_id, chat_id, user_data)
        return success

    async def _handle_fio_correction(self, user_id: int, message_text: str, bot_instance: Bot, chat_id: int,
                                     user_data: dict):
        """Обработка исправления ФИО"""
        success = await self.validate_and_process_input(user_id, message_text, 'fio', bot_instance, chat_id,
                                                        user_data)
        if success:
            self.user_states[user_id] = {'state': 'waiting_confirmation', 'data': user_data}
            await self.send_confirmation_message(bot_instance, user_id, chat_id, user_data)
        return success

    async def _handle_birth_date_correction(self, user_id: int, message_text: str, bot_instance: Bot, chat_id: int,
                                            user_data: dict):
        """Обработка исправления даты рождения"""
        success = await self.validate_and_process_input(user_id, message_text, 'birth_date', bot_instance, chat_id,
                                                        user_data)
        if success:
            self.user_states[user_id] = {'state': 'waiting_confirmation', 'data': user_data}
            await self.send_confirmation_message(bot_instance, user_id, chat_id, user_data)
        return success

    async def _handle_snils_correction(self, user_id: int, message_text: str, bot_instance: Bot, chat_id: int,
                                       user_data: dict):
        """Обработка исправления СНИЛС"""
        success = await self.validate_and_process_input(user_id, message_text, 'snils', bot_instance, chat_id,
                                                        user_data)
        if success:
            self.user_states[user_id] = {'state': 'waiting_confirmation', 'data': user_data}
            await self.send_confirmation_message(bot_instance, user_id, chat_id, user_data)
        return success

    async def _handle_oms_correction(self, user_id: int, message_text: str, bot_instance: Bot, chat_id: int,
                                     user_data: dict):
        """Обработка исправления ОМС"""
        success = await self.validate_and_process_input(user_id, message_text, 'oms', bot_instance, chat_id,
                                                        user_data)
        if success:
            self.user_states[user_id] = {'state': 'waiting_confirmation', 'data': user_data}
            await self.send_confirmation_message(bot_instance, user_id, chat_id, user_data)
        return success

    async def _handle_gender_correction(self, user_id: int, message_text: str, bot_instance: Bot, chat_id: int,
                                        user_data: dict):
        """Обработка исправления пола (текст)"""
        success = await self.validate_and_process_input(user_id, message_text, 'gender', bot_instance, chat_id,
                                                        user_data)
        if success:
            self.user_states[user_id] = {'state': 'waiting_confirmation', 'data': user_data}
            await self.send_confirmation_message(bot_instance, user_id, chat_id, user_data)
        return success

    async def show_esia_option(self, bot_instance: Bot, user_id: int, chat_id: int, user_data: dict):
        """
        Показывает сообщение с опцией входа через ЕСИА, если данные не найдены в региональной системе.
        Один экран: текст и ссылка «Войти через ЕСИА». Мониторинг файла запускается автоматически.
        """
        log_user_event(user_id, "esia_option_shown")
        
        esia_url = generate_esia_url(user_id)
        keyboard = create_keyboard([[{'type': 'link', 'text': 'Войти через ЕСИА', 'url': esia_url}]])
        await bot_instance.send_message(
            chat_id=chat_id,
            text="В региональной системе данные не найдены.\n\nНажмите кнопку ниже и пройдите авторизацию в ЕСИА.",
            attachments=[keyboard] if keyboard else []
        )
        
        self.user_states[user_id] = {
            'state': 'waiting_esia',
            'data': user_data
        }
        asyncio.create_task(self.monitor_esia_file(bot_instance, user_id, chat_id))

    async def handle_esia_check(self, bot_instance: Bot, user_id: int, chat_id: int):
        """
        Вызывается только при нажатии «Я прошёл авторизацию в ЕСИА».
        Запускает мониторинг файла ЕСИА только в этом случае.
        """
        state_info = self.user_states.get(user_id, {})
        if state_info.get('state') != 'waiting_esia':
            await bot_instance.send_message(
                chat_id=chat_id,
                text="Сначала нажмите «Войти через ЕСИА», пройдите авторизацию, затем нажмите «Я прошёл авторизацию в ЕСИА»."
            )
            return
        await self.monitor_esia_file(bot_instance, user_id, chat_id)

    async def monitor_esia_file(self, bot_instance: Bot, user_id: int, chat_id: int):
        """
        Фоновая задача для мониторинга файла ЕСИА после авторизации пользователя
        
        Args:
            bot_instance: Экземпляр бота
            user_id: ID пользователя
            chat_id: ID чата
        """
        log_user_event(user_id, "esia_monitoring_started")
        
        # Отправляем сообщение о начале ожидания
        await bot_instance.send_message(
            chat_id=chat_id,
            text="⏳ Ожидание данных из ЕСИА..."
        )
        
        # Ждем появления файла
        file_path = await wait_for_esia_file(user_id)
        
        if not file_path:
            # Файл не появился после всех попыток
            log_user_event(user_id, "esia_file_not_received")
            await bot_instance.send_message(
                chat_id=chat_id,
                text="Ошибка сервиса. Попробуйте пройти авторизацию позже."
            )
            
            # Показываем стартовое сообщение
            from bot_handlers import send_welcome_message
            await send_welcome_message(bot_instance, chat_id)
            
            # Очищаем состояние
            self.user_states.pop(user_id, None)
            return
        
        # Файл найден, парсим данные (телефон из регистрации — на случай null в файле)
        log_user_event(user_id, "esia_file_received", file_path=file_path)
        user_data = (self.user_states.get(user_id) or {}).get('data', {})
        fallback_phone = user_data.get('phone')
        data = parse_esia_file(file_path, fallback_phone=fallback_phone)
        
        if not data:
            # Ошибка парсинга файла
            log_user_event(user_id, "esia_file_parse_failed", file_path=file_path)
            await bot_instance.send_message(
                chat_id=chat_id,
                text="Ошибка обработки данных из ЕСИА. Попробуйте позже."
            )
            
            # Показываем стартовое сообщение
            from bot_handlers import send_welcome_message
            await send_welcome_message(bot_instance, chat_id)
            
            # Очищаем состояние
            self.user_states.pop(user_id, None)
            return
        
        # Сохраняем данные в БД
        log_user_event(user_id, "esia_data_saving_attempt")
        success = save_esia_data_to_db(user_id, chat_id, data)
        
        if not success:
            # Ошибка сохранения в БД
            log_user_event(user_id, "esia_data_save_failed")
            await bot_instance.send_message(
                chat_id=chat_id,
                text="Ошибка сохранения данных. Попробуйте позже."
            )
            
            # Показываем стартовое сообщение
            from bot_handlers import send_welcome_message
            await send_welcome_message(bot_instance, chat_id)
            
            # Очищаем состояние
            self.user_states.pop(user_id, None)
            return
        
        # Удаляем файл после успешной обработки
        delete_esia_file(file_path)
        
        # Регистрация успешна
        log_user_event(user_id, "esia_registration_completed")
        self.user_states.pop(user_id, None)
        
        # Получаем имя для приветствия
        greeting_name = db.get_user_greeting(user_id)
        
        await bot_instance.send_message(
            chat_id=chat_id,
            text="✅ Успешная регистрация через ЕСИА!\nТеперь вы можете пользоваться всеми функциями бота."
        )
        
        # Показываем главное меню
        from bot_utils import send_main_menu
        await send_main_menu(bot_instance, chat_id, greeting_name)

    async def handle_incorrect_data_info(self, bot_instance: Bot, chat_id: int):
        """Информирование пользователя о действиях при неверных данных из РМИС"""
        await bot_instance.send_message(
            chat_id=chat_id,
            text="ℹ️ Если вы заметили ошибку в своих данных, обратитесь в медицинскую организацию по месту прописки — там смогут внести корректные сведения в вашу медицинскую карту. Сейчас, для дальнейшей регистрации, вы можете нажать кнопку - <<Всё верно, продолжить>>"
        )