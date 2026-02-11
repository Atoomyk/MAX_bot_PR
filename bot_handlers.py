# bot_handlers.py
"""Обработчики событий бота"""
import time
from maxapi.types import BotStarted, MessageCallback, MessageCreated, InputMedia

from bot_config import (
    bot, dp, db, user_states, processed_events,
    sync_service, sync_command_handler, ADMIN_ID,
    registration_handler, reminder_handler, support_handler
)
from bot_utils import (
    anti_duplicate, cleanup_processed_events, create_keyboard,
    send_main_menu, send_other_options_menu
)
from logging_config import log_user_event, log_system_event, log_security_event
from visit_a_doctor.handlers import start_booking, handle_callback as handle_doctor_callback, handle_text_input as handle_doctor_text, get_or_create_context
from visit_a_doctor.states import UserContext as DoctorUserContext
from my_appointments.service import send_my_appointments


# --- ОБРАБОТЧИКИ СОБЫТИЙ ---

async def send_welcome_message(bot, chat_id):
    """Отправляет приветственное сообщение с картинкой"""
    keyboard = create_keyboard([[
        {'type': 'callback', 'text': 'Продолжить', 'payload': "start_continue"}
    ]])

    attachments = []
    import os
    image_path = os.path.join(os.getcwd(), 'assets', 'start_foto.png')
    if os.path.exists(image_path):
        attachments.append(InputMedia(path=image_path))
    
    if keyboard:
        attachments.append(keyboard)

    await bot.send_message(
        chat_id=chat_id,
        text='Здравствуйте! 👩‍⚕️\n\nВас приветствует чат-бот "Цифровое здравоохранение Севастополя"!\nТут можно быстро и легко:\n📌 Записаться к врачу\n📌 Получать уведомления о записях к врачам\n📌 Отменять приёмы при необходимости\n📌 Обратиться в онлайн-чат поддержки по любому вопросу',
        attachments=attachments
    )


@dp.bot_started()
@anti_duplicate()
async def bot_started(event: BotStarted):
    """Обработка запуска бота"""
    chat_id = int(event.chat_id)
    # Предполагаем, что user_id доступен через event.from_user.user_id, 
    # если нет - используем chat_id как временное решение, но архитектурно нужен user_id
    try:
        user_id = int(event.from_user.user_id)
    except AttributeError:
        # Fallback если from_user недоступен в BotStarted (зависит от версии API)
        user_id = chat_id

    log_user_event(user_id, "bot_started")

    current_time = time.time()
    # Используем user_id для анти-спама
    last_bot_start = processed_events.get(user_id, {}).get('last_bot_start', 0)

    if current_time - last_bot_start < 30:
        log_user_event(user_id, "bot_started_ignored_duplicate")
        return

    if user_id not in processed_events:
        processed_events[user_id] = {}
    processed_events[user_id]['last_bot_start'] = current_time

    try:
        # Обновляем last_chat_id при старте
        if db.is_user_registered(user_id):
            db.update_last_chat_id(user_id, chat_id)
            greeting_name = db.get_user_greeting(user_id)
            log_user_event(user_id, "already_registered")
            await send_main_menu(event.bot, chat_id, greeting_name) # Меню в чат
        else:
            log_user_event(user_id, "new_user_detected")
            await send_welcome_message(event.bot, chat_id) # Приветствие в чат

    except Exception as e:
        log_system_event("bot_started", "message_send_failed", error=str(e), user_id=user_id)


@dp.message_callback()
@anti_duplicate()
async def message_callback(event: MessageCallback):
    """Обработка нажатий на инлайн-кнопки"""
    try:
        if len(processed_events) > 1000:
            cleanup_processed_events()

        chat_id = int(event.message.recipient.chat_id)
        # Извлекаем user_id
        try:
             user_id = int(event.from_user.user_id)
        except AttributeError:
             # Fallback, хотя event.from_user должен быть
             user_id = int(event.message.sender.user_id) if hasattr(event.message, 'sender') else chat_id
        
        # Обновляем последний чат
        if db.is_user_registered(user_id):
             db.update_last_chat_id(user_id, chat_id)

        payload = event.callback.payload

        # --- ТМК Module ---
        if payload.startswith('tmk_consent_'):
            from bot_config import tmk_database, tmk_bot, tmk_reminder_service
            if tmk_database and tmk_bot:
                from tmk.handlers import handle_tmk_consent
                handled = await handle_tmk_consent(event, tmk_bot, tmk_database, tmk_reminder_service)
                if handled:
                    return
        
        # --- Visit Doctor Module ---
        if payload == 'start_visit_doctor':
            log_user_event(user_id, "visit_doctor_start")
            if not db.is_user_registered(user_id):
                keyboard = create_keyboard([[
                    {'type': 'callback', 'text': 'Начать регистрацию', 'payload': "start_continue"}
                ]])
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="❌ Для записи к врачу необходимо сначала зарегистрироваться.",
                    attachments=[keyboard] if keyboard else []
                )
                return
            await start_booking(event.bot, user_id, chat_id)
            return

        if payload == 'my_appointments':
            log_user_event(user_id, "my_appointments_opened")
            if not db.is_user_registered(user_id):
                keyboard = create_keyboard([[
                    {'type': 'callback', 'text': 'Начать регистрацию', 'payload': "start_continue"}
                ]])
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="❌ Для доступа к записям необходима регистрация.",
                    attachments=[keyboard] if keyboard else []
                )
                return
            await send_my_appointments(event.bot, user_id, chat_id)
            return
            
        if payload.startswith('doc_'):
            log_user_event(user_id, "visit_doctor_action", payload=payload)
            await handle_doctor_callback(event.bot, user_id, chat_id, payload)
            return
        # ---------------------------

        # Логирование button_pressed будет происходить только для неизвестных payload
        # Специфичные события логируются в соответствующих обработчиках

        # Обработка callback-ов для записей к врачу
        if payload.startswith("view_appointment:"):
            if not db.is_user_registered(user_id):
                await event.bot.send_message(chat_id=chat_id, text="❌ Для доступа к записям необходима регистрация.")
                return
            try:
                appointment_id = int(payload.split(":")[1])
                log_user_event(user_id, "appointment_details_viewed", appointment_id=appointment_id)
                if sync_service and sync_service.notifier:
                    await sync_service.notifier.send_appointment_details(user_id, appointment_id)
                else:
                    await event.bot.send_message(
                        chat_id=chat_id,
                        text="Сервис записей временно недоступен. Пожалуйста, попробуйте позже."
                    )
            except (ValueError, IndexError):
                log_system_event("appointment_view_error", "invalid_appointment_id", payload=payload, user_id=user_id)
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="Ошибка при обработке запроса. Пожалуйста, попробуйте еще раз."
                )
            return

        elif payload == "view_appointments_list":
            log_user_event(user_id, "appointments_list_viewed")
            if not db.is_user_registered(user_id):
                await event.bot.send_message(chat_id=chat_id, text="❌ Для доступа к записям необходима регистрация.")
                return
            if sync_service and sync_service.notifier:
                await sync_service.notifier.send_appointments_list(user_id)
            else:
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="Сервис записей временно недоступен. Пожалуйста, попробуйте позже."
                )
            return

        elif payload.startswith("cancel_appointment:"):
            # Обработка отмены записи
            if not db.is_user_registered(user_id):
                await event.bot.send_message(chat_id=chat_id, text="❌ Для отмены записей необходима регистрация.")
                return
            
            if payload == "cancel_appointment:stub":
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="⏳ Функция отмены записи в настоящее время недоступна.\n\n"
                         "Для отмены записи обратитесь в регистратуру медицинского учреждения "
                         "или воспользуйтесь порталом Госуслуги."
                )
                return
            
            # Извлекаем ID записи
            try:
                appointment_id = int(payload.split(":")[1])
            except (ValueError, IndexError):
                log_user_event(user_id, "appointment_cancel_error", error="invalid_payload", payload=payload)
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="❌ Ошибка: некорректный идентификатор записи."
                )
                return
            
            # Проверяем существование записи и её статус
            # Импортируем sync_service динамически, так как он может быть инициализирован позже
            from bot_config import sync_service as sync_service_check
            if not sync_service_check or not hasattr(sync_service_check, 'appointments_db') or not sync_service_check.appointments_db:
                log_user_event(user_id, "appointment_cancel_error", error="service_unavailable")
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="❌ Сервис записей временно недоступен. Попробуйте позже."
                )
                return
            
            # Используем проверенный sync_service
            sync_service = sync_service_check
            
            appointment = sync_service.appointments_db.get_appointment_by_id_with_status(
                appointment_id, user_id
            )
            
            if not appointment:
                log_user_event(user_id, "appointment_cancel_error", 
                             error="not_found", appointment_id=appointment_id)
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="❌ Запись не найдена или не принадлежит вам."
                )
                return
            
            if appointment['status'] == 'cancelled':
                log_user_event(user_id, "appointment_cancel_error", 
                             error="already_cancelled", appointment_id=appointment_id)
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="ℹ️ Эта запись уже отменена."
                )
                return
            
            # Показываем подтверждение
            log_user_event(user_id, "appointment_cancel_confirmation_shown", appointment_id=appointment_id)
            
            from maxapi.types import CallbackButton
            from maxapi.utils.inline_keyboard import ButtonsPayload, AttachmentType
            from maxapi.types import Attachment
            
            confirmation_buttons = [
                [
                    CallbackButton(
                        text="✅ Да",
                        payload=f"cancel_appointment_confirm:{appointment_id}"
                    ),
                    CallbackButton(
                        text="⬅️ Назад",
                        payload="cancel_appointment_back"
                    )
                ]
            ]
            
            buttons_payload = ButtonsPayload(buttons=confirmation_buttons)
            keyboard = Attachment(
                type=AttachmentType.INLINE_KEYBOARD,
                payload=buttons_payload
            )
            
            await event.bot.send_message(
                chat_id=chat_id,
                text="⚠️ Вы подтверждаете отмену записи?\n\n"
                     "При нажатии кнопки «Да», запись будет отменена без возможности восстановления.",
                attachments=[keyboard]
            )
            return
        
        elif payload.startswith("cancel_appointment_confirm:"):
            # Подтверждение отмены записи
            if not db.is_user_registered(user_id):
                await event.bot.send_message(chat_id=chat_id, text="❌ Для отмены записей необходима регистрация.")
                return
            
            try:
                appointment_id = int(payload.split(":")[1])
            except (ValueError, IndexError):
                log_user_event(user_id, "appointment_cancel_error", error="invalid_confirm_payload", payload=payload)
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="❌ Ошибка: некорректный идентификатор записи."
                )
                return
            
            # Импортируем sync_service динамически
            from bot_config import sync_service as sync_service_check
            if not sync_service_check or not hasattr(sync_service_check, 'appointments_db') or not sync_service_check.appointments_db:
                log_user_event(user_id, "appointment_cancel_error", error="service_unavailable")
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="❌ Сервис записей временно недоступен. Попробуйте позже."
                )
                return
            
            # Используем проверенный sync_service
            sync_service = sync_service_check
            
            # Получаем данные записи, чтобы отправить SOAP-запрос отмены
            appointment_info = sync_service.appointments_db.get_appointment_by_id_with_status(
                appointment_id, user_id
            )

            if not appointment_info:
                log_user_event(user_id, "appointment_cancel_error",
                             error="not_found", appointment_id=appointment_id)
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="❌ Запись не найдена или не принадлежит вам."
                )
                return

            if appointment_info.get('status') == 'cancelled':
                log_user_event(user_id, "appointment_cancel_error",
                             error="already_cancelled", appointment_id=appointment_id)
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="ℹ️ Эта запись уже отменена."
                )
                return

            appointment_data = appointment_info.get('data') or {}
            book_id_mis = appointment_data.get('Book_Id_Mis')
            cancel_reason = getattr(getattr(sync_service, 'cancel_service', None), 'DEFAULT_REASON', "CANCELED_BY_PATIENT")

            if not book_id_mis:
                log_user_event(
                    user_id,
                    "appointment_cancel_failed",
                    error="missing_book_id_mis",
                    appointment_id=appointment_id
                )
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="❌ Не удалось отменить запись: отсутствует идентификатор записи (Book_Id_Mis) во внешней системе.\n"
                         "Попробуйте отменить запись по телефону 122."
                )
                return

            cancel_service = getattr(sync_service, 'cancel_service', None)
            if not cancel_service:
                log_system_event("appointment", "cancel_failed",
                                appointment_id=appointment_id,
                                error="cancel_service_unavailable",
                                user_id=user_id)
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="❌ Сервис отмены временно недоступен. Попробуйте позже."
                )
                return

            # Отправляем SOAP-запрос на отмену записи
            cancel_result = await cancel_service.send_cancel_request(
                book_id_mis=book_id_mis,
                canceled_reason=cancel_reason
            )

            if not cancel_result.get('success'):
                log_user_event(user_id, "appointment_cancel_failed",
                             error=cancel_result.get('error', 'soap_error'),
                             appointment_id=appointment_id)
                log_system_event("appointment", "cancel_failed",
                                appointment_id=appointment_id,
                                error=cancel_result.get('error', cancel_result),
                                user_id=user_id)
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="❌ Не удалось отменить запись во внешней системе. Попробуйте позже."
                )
                return

            # Проверяем статус-код в ответе внешней системы (например, RECORD_NOT_FOUND)
            response_text = cancel_result.get('response', '') or ''
            import re
            success_match = re.search(
                r"<(?:\w+:)?Status_Code>\s*SUCCESS\s*</(?:\w+:)?Status_Code>",
                response_text
            )
            if not success_match:
                log_user_event(
                    user_id,
                    "appointment_cancel_failed",
                    error="external_status_not_success",
                    appointment_id=appointment_id,
                    external_response=response_text[:500]
                )
                log_system_event(
                    "appointment",
                    "cancel_failed_external_status",
                    appointment_id=appointment_id,
                    user_id=user_id,
                    external_response=response_text[:500]
                )
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="❌ Внешняя система вернула ошибку отмены (запись не найдена или уже отменена)."
                )
                return

            # Если SOAP-запрос успешен — фиксируем отмену в БД
            result = sync_service.appointments_db.cancel_appointment(appointment_id, user_id, cancelled_by='user_cancel')
            
            if result['success']:
                log_user_event(user_id, "appointment_cancelled", appointment_id=appointment_id)
                log_system_event("appointment", "cancelled", 
                               appointment_id=appointment_id, chat_id=chat_id)
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="✅ Запись была отменена."
                )
            else:
                log_user_event(user_id, "appointment_cancel_failed", 
                             error=result.get('error', 'unknown'), appointment_id=appointment_id)
                log_system_event("appointment", "cancel_failed", 
                               appointment_id=appointment_id, 
                               error=result.get('error', 'unknown'),
                               chat_id=chat_id)
                await event.bot.send_message(
                    chat_id=chat_id,
                    text=f"❌ {result.get('error', 'Не удалось отменить запись.')}"
                )
            return
        
        elif payload == "cancel_appointment_back":
            # Возврат в главное меню
            log_user_event(user_id, "appointment_cancel_cancelled")
            
            # Сбрасываем контекст записи к врачу
            ctx = await get_or_create_context(user_id)
            ctx.step = "INIT"
            
            if db.is_user_registered(user_id):
                greeting_name = db.get_user_greeting(user_id)
                await send_main_menu(event.bot, chat_id, greeting_name)
            else:
                await send_welcome_message(event.bot, chat_id)

            return

        # Обработка админских callback для синхронизации
        if sync_command_handler and chat_id == ADMIN_ID: # ADMIN_ID может быть использован как user_id или chat_id, тут не критично
            if payload.startswith("sync_"):
                log_system_event("admin_callback", "sync_callback_received", payload=payload, user_id=user_id)
                handled = await sync_command_handler.handle_callback(event, payload)
                if handled:
                    log_system_event("admin_callback", "sync_callback_handled", payload=payload, user_id=user_id)
                    return

        # Обработка callback-ов регистрации
        if payload == "start_continue":
            log_user_event(user_id, "registration_start_clicked")
            # ПЕРЕДАЕМ И user_id И chat_id
            await registration_handler.send_agreement_message(event.bot, user_id, chat_id)

        elif payload == "agreement_accepted":
            log_security_event(user_id, "consent_accepted")
            await registration_handler.start_registration_process(event.bot, user_id, chat_id)

        elif payload == "confirm_phone":
            # Логирование phone_confirmed происходит в handle_phone_confirmation
            await registration_handler.handle_phone_confirmation(event.bot, user_id, chat_id)

        elif payload == "esia_check_data":
            await registration_handler.handle_esia_check(event.bot, user_id, chat_id)

        elif payload == "reject_phone":
            # Логирование phone_rejected происходит в handle_incorrect_phone
            await registration_handler.handle_incorrect_phone(event.bot, user_id, chat_id)

        elif payload == "correct_fio":
            log_user_event(user_id, "fio_correction_requested")
            await registration_handler.handle_data_correction(event.bot, user_id, chat_id, 'fio')

        elif payload == "correct_birth_date":
            log_user_event(user_id, "birth_date_correction_requested")
            await registration_handler.handle_data_correction(event.bot, user_id, chat_id, 'birth_date')

        elif payload == "correct_snils":
            log_user_event(user_id, "snils_correction_requested")
            await registration_handler.handle_data_correction(event.bot, user_id, chat_id, 'snils')

        elif payload == "correct_oms":
            log_user_event(user_id, "oms_correction_requested")
            await registration_handler.handle_data_correction(event.bot, user_id, chat_id, 'oms')

        elif payload == "correct_gender":
            log_user_event(user_id, "gender_correction_requested")
            await registration_handler.handle_data_correction(event.bot, user_id, chat_id, 'gender')

        elif payload == "gender_male":
            await registration_handler.handle_gender_choice(event.bot, user_id, chat_id, "Мужской")

        elif payload == "gender_female":
            await registration_handler.handle_gender_choice(event.bot, user_id, chat_id, "Женский")

        elif payload == "reg_incorrect_data":
             await registration_handler.handle_incorrect_data_info(event.bot, chat_id)

        elif payload.startswith("reg_identity_"):
            selection = payload.replace("reg_identity_", "")
            await registration_handler.handle_identity_selection(event.bot, user_id, chat_id, selection)

        elif payload == "reg_back_to_list":
            await registration_handler.handle_back_to_list(event.bot, user_id, chat_id)

        elif payload == "confirm_data":
            log_user_event(user_id, "registration_data_confirmed")
            greeting_name = await registration_handler.handle_data_confirmation(event.bot, user_id, chat_id)
            if greeting_name:
                await send_main_menu(event.bot, chat_id, greeting_name)

        elif payload == "get_user_manual":
            log_user_event(user_id, "user_manual_requested")
            import os
            manual_path = os.path.join(os.getcwd(), 'assets', 'USER_MANUAL.md')
            attachments = []
            if os.path.exists(manual_path):
                attachments.append(InputMedia(path=manual_path))
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="📖 Руководство пользователя:",
                    attachments=attachments
                )
            else:
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="❌ Файл руководства не найден."
                )
            return

        # Обработка основных callback-ов
        elif payload == "other_options":
            log_user_event(user_id, "other_options_menu_opened")
            await send_other_options_menu(event.bot, chat_id)

        elif payload == "back_to_main" or payload == "main_menu":
            log_user_event(user_id, "back_to_main_menu")
            
            # Сбрасываем контекст записи к врачу
            ctx = await get_or_create_context(user_id)
            ctx.step = "INIT"
            
            if db.is_user_registered(user_id):
                greeting_name = db.get_user_greeting(user_id)
                await send_main_menu(event.bot, chat_id, greeting_name)
            else:
                await send_welcome_message(event.bot, chat_id)
            
            return

        # Управление напоминаниями
        elif payload == "reminders_settings":
            log_user_event(user_id, "reminders_settings_opened")
            if not db.is_user_registered(user_id):
                await event.bot.send_message(chat_id=chat_id, text="❌ Для доступа к настройкам необходима регистрация.")
                return
            await reminder_handler.send_reminder_settings(event.bot, user_id, chat_id)
            return

        elif payload == "reminders_yes":
            log_user_event(user_id, "reminders_enabled")
            if not db.is_user_registered(user_id):
                await event.bot.send_message(chat_id=chat_id, text="❌ Для доступа к настройкам необходима регистрация.")
                return
            await reminder_handler.enable_reminders(event.bot, user_id, chat_id)
            return

        elif payload == "reminders_no":
            log_user_event(user_id, "reminders_disabled")
            if not db.is_user_registered(user_id):
                await event.bot.send_message(chat_id=chat_id, text="❌ Для доступа к настройкам необходима регистрация.")
                return
            await reminder_handler.disable_reminders(event.bot, user_id, chat_id)
            return

        elif payload == "reminders_back":
            log_user_event(user_id, "reminders_back_clicked")
            if not db.is_user_registered(user_id):
                await event.bot.send_message(chat_id=chat_id, text="❌ Для доступа к настройкам необходима регистрация.")
                return
            await reminder_handler.go_back(event.bot, user_id, chat_id)
            return

        # Онлайн чат с поддержкой
        elif payload.startswith("start_chat:"):
            log_user_event(user_id, "admin_start_chat_clicked")
            try:
                user_id_to_connect = int(payload.split(":")[1])
                await support_handler.connect_admin_to_chat(event.bot, user_id, user_id_to_connect, admin_chat_id=chat_id)
            except (ValueError, IndexError):
                await event.bot.send_message(chat_id=chat_id, text="❌ Ошибка в идентификаторе чата.")
            return

        elif payload == "support_request":
            log_user_event(user_id, "support_chat_requested")
            if db.is_user_registered(user_id):
                greeting_name = db.get_user_greeting(user_id)
                user_phone = ""

                try:
                    if hasattr(db, 'get_user_phone'):
                        user_phone = db.get_user_phone(user_id)
                    else:
                        user_data = db.get_user_data(user_id)
                        user_phone = user_data.get('phone', '') if user_data else ''
                except Exception as phone_error:
                    log_system_event("phone_retrieval_error", str(phone_error), user_id=user_id)
                    user_phone = "Не указан"

                user_data = {
                    'fio': greeting_name,
                    'phone': user_phone
                }

                await support_handler.handle_support_request(event.bot, user_id, chat_id, user_data)
            else:
                keyboard = create_keyboard([[
                    {'type': 'callback', 'text': 'Начать регистрацию', 'payload': "start_continue"}
                ]])
                await event.bot.send_message(
                    chat_id=chat_id,
                    text="❌ Для использования онлайн-чата с поддержкой необходимо сначала зарегистрироваться.",
                    attachments=[keyboard] if keyboard else []
                )

    except Exception as e:
        chat_id_str = str(event.message.recipient.chat_id) if hasattr(event, 'message') and hasattr(event.message, 'recipient') else 'unknown'
        log_system_event("callback_error", str(e), chat_id=chat_id_str)
        user_states.pop(chat_id_str, None)
        try:
            await event.bot.send_message(
                chat_id=event.message.recipient.chat_id,
                text="Произошла ошибка при обработке запроса. Пожалуйста, попробуйте еще раз."
            )
        except Exception as send_error:
            log_system_event("callback_error_send_failed", str(send_error), chat_id=chat_id_str)


@dp.message_created()
@anti_duplicate()
async def handle_message(event: MessageCreated):
    """Обработка всех текстовых сообщений"""
    try:
        if len(processed_events) > 1000:
            cleanup_processed_events()

        chat_id = int(event.message.recipient.chat_id)
        # Извлекаем user_id
        try:
             user_id = int(event.from_user.user_id)
        except AttributeError:
             # Fallback
             user_id = int(event.message.sender.user_id) if hasattr(event.message, 'sender') else chat_id

        # Обновляем последний чат
        if db.is_user_registered(user_id):
             db.update_last_chat_id(user_id, chat_id)

        is_admin = (user_id == ADMIN_ID or chat_id == ADMIN_ID) if ADMIN_ID else False

        if not event.message.body:
            return

        # --- Visit Doctor Module ---
        # Проверяем, находится ли пользователь в сценарии записи к врачу
        if event.message.body.text:
            try:
                # Используем user_id для контекста
                ctx = await get_or_create_context(user_id)
                if ctx.step != "INIT":
                    text_val = event.message.body.text
                    if text_val.lower() in ['/start', 'отмена', 'стоп', 'выйти']:
                        ctx.step = "INIT"
                        # Проваливаемся дальше, чтобы показать главное меню
                    else:
                        log_user_event(user_id, "visit_doctor_text_input")
                        await handle_doctor_text(event.bot, user_id, chat_id, text_val)
                        return
            except Exception as e:
                log_system_event("visit_doctor_module", "context_error", error=str(e), user_id=user_id)
        # ---------------------------

        # Получаем attachments из сообщения
        attachments = event.message.body.attachments if hasattr(event.message.body, 'attachments') else None
        
        # Проверяем наличие изображений в attachments
        has_image = False
        if attachments:
            for attachment in attachments:
                if hasattr(attachment, 'type'):
                    if attachment.type == "image" or (hasattr(attachment, 'type') and str(attachment.type).lower() == "image"):
                        has_image = True
                        break

        # Обработка админских команд для синхронизации
        # Проверяем только команды, начинающиеся с /admin_, чтобы не обрабатывать обычные сообщения админа
        if is_admin and event.message.body:
            message_text = event.message.body.text.strip() if event.message.body.text else ""
            
            # Обрабатываем только команды синхронизации (если есть текст)
            if message_text and message_text.startswith("/admin_"):
                log_system_event("admin_command", "command_received", command=message_text, user_id=user_id)
                
                # Импортируем sync_command_handler динамически, так как он может быть инициализирован позже
                from bot_config import sync_command_handler
                
                if sync_command_handler:
                    handled = await sync_command_handler.handle_message(event)
                    if handled:
                        log_system_event("admin_command", "command_handled", command=message_text, user_id=user_id)
                        return
                else:
                    log_system_event("admin_command", "sync_handler_not_available", command=message_text, user_id=user_id)

            # Обработка сообщений администратора через support_handler (включая изображения)
            # Обрабатываем если есть текст или изображение
            if message_text or has_image:
                # Админ отправляет сообщение в поддержку (возможно как ответ)
                # Тут надо проверить логику support_handler.process_admin_message
                processed = await support_handler.process_admin_message(
                    event.bot, user_id, message_text, attachments
                )
                if processed:
                    log_system_event("admin_command", "handled_by_support", command=message_text or "[изображение]", user_id=user_id)
                    return

        if event.message.body.attachments:
            # Обработка контакта при регистрации
            contact_processed = await registration_handler.process_contact_message(
                event, user_id, chat_id
            )
            if contact_processed:
                return

        if not event.message.sender:
            return

        # Получаем текст сообщения (может быть пустым, если только изображение)
        message_text = event.message.body.text.strip() if event.message.body.text else ""
        
        # Пропускаем обработку, если нет ни текста, ни изображения
        if not message_text and not has_image:
            return

        # Логируем сообщения пользователей
        is_admin_msg = (user_id == ADMIN_ID) if ADMIN_ID else False
        if not (is_admin_msg and message_text and message_text.startswith("/")):
            log_user_event(user_id, "message_sent", text=message_text or "[изображение]")

        if not db.is_user_registered(user_id) and str(user_id) not in user_states and user_id not in user_states:
            # Проверка user_states на int и str ключи пока рефакторинг идет
            log_user_event(user_id, "message_ignored_unregistered")
            await send_welcome_message(event.bot, chat_id)
            return

        # Обработка регистрации только если есть текст
        if message_text:
            registration_processed = await registration_handler.process_text_input(
                user_id, message_text, event.bot, chat_id
            )

            if registration_processed:
                return

        # Обработка сообщений в чате поддержки (включая изображения)
        chat_processed = await support_handler.process_user_message(
            event.bot, user_id, message_text, attachments
        )
        if chat_processed:
            return

        if db.is_user_registered(user_id):
            greeting_name = db.get_user_greeting(user_id)
            await send_main_menu(event.bot, chat_id, greeting_name)
            return

        # Проверка состояния пользователя (int)
        if not user_states.get(user_id) and not user_states.get(str(user_id)):
             await send_welcome_message(event.bot, chat_id)

    except Exception as e:
        chat_id_recip = event.message.recipient.chat_id if hasattr(event, 'message') and hasattr(event.message, 'recipient') else 'unknown'
        log_system_event("message_handler_error", str(e), chat_id=str(chat_id_recip))
        # Очистка состояния при ошибке
        user_states.pop(user_id, None) if 'user_id' in locals() else None
        
        try:
            await event.bot.send_message(
                chat_id=event.message.recipient.chat_id,
                text="Произошла ошибка при обработке сообщения. Пожалуйста, попробуйте еще раз."
            )
        except Exception as send_error:
            log_system_event("message_error_send_failed", str(send_error), chat_id=str(chat_id_recip))
