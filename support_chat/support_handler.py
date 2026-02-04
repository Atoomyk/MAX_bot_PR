# support_handler.py
import os
import json
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, List, Any
from dataclasses import dataclass, asdict
import time

from dotenv import load_dotenv
from maxapi.types import Attachment, OtherAttachmentPayload, CallbackButton, ButtonsPayload
from maxapi.utils.inline_keyboard import AttachmentType

# Импорт системы логирования
from logging_config import log_user_event, log_system_event, log_security_event
# Импорт утилит для клавиатур (предполагаем, что bot_utils доступен, так как он в корне)
# Но лучше не импортировать bot_utils, чтобы избежать циклических зависимостей, 
# если bot_handler будет импортировать support_handler.
# Используем здесь прямую генерацию клавиатур или перенесем create_keyboard в отдельный модуль.
# Используем здесь прямую генерацию клавиатур или перенесем create_keyboard в отдельный модуль.
# Пока используем maxapi напрямую.
from user_database import db

# Загрузка переменных окружения
load_dotenv()

# Настройки из .env
ADMIN_ID = os.getenv("ADMIN_ID")
if ADMIN_ID:
    ADMIN_ID = int(ADMIN_ID)

# Константы
TICKETS_DIR = Path("tickets")
INACTIVITY_TIMEOUT = 3600  # 1 час в секундах
LOG_RETENTION_DAYS = 30  # Хранить логи 30 дней


@dataclass
class ChatMessage:
    """Структура сообщения в чате"""
    from_user: str  # "user" или "admin"
    text: str
    time: str
    image_url: Optional[str] = None  # URL изображения, если есть


@dataclass
class ChatLog:
    """Структура лога чата"""
    user_id: int
    user_name: str
    user_phone: str
    admin_id: Optional[int]
    start_time: str
    end_time: Optional[str]
    messages: List[ChatMessage]


class SupportHandler:
    def __init__(self, user_states: Dict):
        self.user_states = user_states
        self.admin_id = ADMIN_ID

        # Структуры для управления чатами
        self.active_chats: Dict[int, Dict] = {}  # user_id -> chat_info
        self.waiting_queue: List[Dict] = []  # Очередь ожидающих пользователей
        self.admin_active_chat: Optional[int] = None  # user_id с которым общается админ
        self.chat_logs: Dict[int, ChatLog] = {}  # Активные логи чатов

        # Создаем папку для логов
        self._ensure_tickets_dir()

        # Запускаем фоновую задачу для проверки неактивности
        self._cleanup_task = None

    def _ensure_tickets_dir(self):
        """Создает папку для логов если ее нет"""
        if not TICKETS_DIR.exists():
            TICKETS_DIR.mkdir(exist_ok=True)
            log_system_event("support_chat", "tickets_dir_created")

    async def start_cleanup_task(self):
        """Запускает фоновую задачу для очистки"""
        self._cleanup_task = asyncio.create_task(self._cleanup_worker())

    async def _cleanup_worker(self):
        """Фоновая задача для проверки неактивности и очистки старых логов"""
        while True:
            try:
                await self._check_inactive_chats()
                await self._cleanup_old_logs()
                await asyncio.sleep(300)  # Проверяем каждые 5 минут
            except Exception as e:
                log_system_event("support_chat", "cleanup_error", error=str(e))
                await asyncio.sleep(60)

    async def _check_inactive_chats(self):
        """Проверяет и завершает неактивные чаты"""
        current_time = time.time()
        chats_to_end = []

        for user_id, chat_info in list(self.active_chats.items()):
            last_activity = chat_info.get('last_activity', 0)
            if current_time - last_activity > INACTIVITY_TIMEOUT:
                chats_to_end.append(user_id)

        for user_id in chats_to_end:
            await self._auto_end_chat(user_id)

    async def _auto_end_chat(self, user_id: int):
        """Автоматическое завершение чата по неактивности"""
        chat_info = self.active_chats.get(user_id)
        if not chat_info:
            return

        # Получаем бота для отправки уведомлений
        bot = self._get_bot()
        if not bot:
            log_system_event("support_chat", "auto_end_no_bot", user_id=user_id)
            return

        # Завершаем чат
        await self._end_chat(
            bot=bot,
            user_id=user_id,
            ended_by="system",
            reason="inactivity"
        )

        log_system_event("support_chat", "auto_ended", user_id=user_id)

    async def _cleanup_old_logs(self):
        """Удаляет старые логи (старше 30 дней)"""
        try:
            cutoff_date = datetime.now() - timedelta(days=LOG_RETENTION_DAYS)

            for log_file in TICKETS_DIR.glob("*.json"):
                try:
                    # Пытаемся получить дату из имени файла
                    file_date_str = log_file.stem.split('_')[-1]
                    file_date = datetime.strptime(file_date_str, "%Y-%m-%d_%H-%M")

                    if file_date < cutoff_date:
                        log_file.unlink()
                        log_system_event("support_chat", "old_log_deleted", file=log_file.name)
                except (ValueError, IndexError):
                    continue
        except Exception as e:
            log_system_event("support_chat", "cleanup_logs_error", error=str(e))

    def _create_log_filename(self, user_id: int) -> str:
        """Создает имя файла для лога (с timestamp начала чата)"""
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        return f"{user_id}_{timestamp}.json"

    def _get_log_filename(self, user_id: int) -> Optional[str]:
        """Получает имя файла лога для чата (создает если не существует)"""
        chat_info = self.active_chats.get(user_id)
        if not chat_info:
            return None
        
        # Если имя файла уже создано, возвращаем его
        if 'log_filename' in chat_info:
            return chat_info['log_filename']
        
        # Создаем новое имя файла и сохраняем его
        filename = self._create_log_filename(user_id)
        chat_info['log_filename'] = filename
        return filename

    def _save_chat_log(self, user_id: int, end_chat: bool = False):
        """Сохраняет лог чата в файл (обновляет один и тот же файл)"""
        try:
            if user_id not in self.chat_logs:
                return

            chat_log = self.chat_logs[user_id]

            if end_chat:
                chat_log.end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Конвертируем в словарь
            log_dict = {
                "user_id": chat_log.user_id,
                "user_name": chat_log.user_name,
                "user_phone": chat_log.user_phone,
                "admin_id": chat_log.admin_id,
                "start_time": chat_log.start_time,
                "end_time": chat_log.end_time,
                "messages": [
                    {
                        "from": msg.from_user,
                        "text": msg.text,
                        "time": msg.time,
                        "image_url": msg.image_url
                    }
                    for msg in chat_log.messages
                ]
            }

            # Получаем имя файла (используем сохраненное или создаем новое)
            filename = self._get_log_filename(user_id)
            if not filename:
                # Если чат уже завершен, создаем имя файла на основе start_time
                start_time_obj = datetime.strptime(chat_log.start_time, "%Y-%m-%d %H:%M:%S")
                timestamp = start_time_obj.strftime("%Y-%m-%d_%H-%M-%S")
                filename = f"{user_id}_{timestamp}.json"
            
            filepath = TICKETS_DIR / filename

            # Сохраняем/обновляем файл
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(log_dict, f, ensure_ascii=False, indent=2)

            if end_chat:
                log_user_event(str(user_id), "chat_log_saved", filename=filename)
                # Удаляем из памяти после сохранения
                del self.chat_logs[user_id]
            else:
                log_user_event(str(user_id), "chat_log_updated", filename=filename)

        except Exception as e:
            log_system_event("support_chat", "save_log_error", error=str(e), user_id=user_id)

    async def handle_support_request(self, bot, user_id: int, chat_id: int, user_data: dict):
        """Обрабатывает запрос на поддержку - запускает онлайн чат"""

        # Проверяем, не в чате ли уже пользователь
        if user_id in self.active_chats:
            await bot.send_message(
                chat_id=chat_id,
                text="Вы уже находитесь в чате с техподдержкой.\n\nЧтобы выйти — отправьте цифру 0."
            )
            return

        # Проверяем, есть ли свободный оператор
        if self.admin_active_chat is not None:
            # Оператор занят - добавляем в очередь
            self.waiting_queue.append({
                'user_id': user_id,
                'chat_id': chat_id,
                'user_data': user_data,
                'timestamp': time.time()
            })

            await bot.send_message(
                chat_id=chat_id,
                text="⏳ Оператор занят. Вы добавлены в очередь ожидания.\n\nКак только оператор освободится, с вами свяжутся."
            )

            log_user_event(user_id, "added_to_waiting_queue")
            return

        # Создаем новый чат
        self._create_new_chat(user_id, chat_id, user_data)

        # Отправляем сообщение пользователю
        await bot.send_message(
            chat_id=chat_id,
            text="⏳ Ждем подключения оператора...\n\nОпишите вашу проблему или вопрос. Как только оператор подключится, он увидит все ваши сообщения.\n\nЧтобы выйти из чата — отправьте цифру 0."
        )

        # Уведомляем администратора
        await self._notify_admin_new_chat(bot, user_id, chat_id, user_data)

        log_user_event(user_id, "chat_requested")


    def _create_new_chat(self, user_id: int, chat_id: int, user_data: dict):
        """Создает новую структуру чата"""
        # Создаем лог чата
        chat_log = ChatLog(
            user_id=user_id,
            user_name=user_data.get('fio', 'Неизвестно'),
            user_phone=user_data.get('phone', 'Не указан'),
            admin_id=None,
            start_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            end_time=None,
            messages=[]
        )

        # Сохраняем в памяти
        self.chat_logs[user_id] = chat_log

        # Создаем имя файла для лога один раз при создании чата
        log_filename = self._create_log_filename(user_id)

        # Добавляем в активные чаты
        self.active_chats[user_id] = {
            'chat_id': chat_id,
            'user_data': user_data,
            'last_activity': time.time(),
            'waiting_for_admin': True,
            'messages_queue': [],  # Сообщения, отправленные до подключения админа
            'log_filename': log_filename  # Сохраняем имя файла для этого чата
        }

        log_user_event(user_id, "chat_created", log_filename=log_filename)

    async def _notify_admin_new_chat(self, bot, user_id: int, chat_id: int, user_data: dict):
        """Уведомляет администратора о новом чате"""
        if not self.admin_id:
            log_system_event("support_chat", "no_admin_id")
            return

        if not bot:
            log_system_event("support_chat", "no_bot_for_notification", user_id=user_id)
            return

        try:
            # Получаем chat_id администратора
            admin_chat_id = db.get_last_chat_id(self.admin_id)
            if not admin_chat_id:
                log_system_event("support_chat", "admin_chat_id_not_found_for_notification", admin_id=self.admin_id)
                # Если не нашли чат админа, можно попробовать записать в лог или отправить в "никуда", 
                # но лучше просто выйти, так как отправить некому.
                return

            message = (
                f"🆕 Новый чат от пользователя:\n\n"
                f"👤 ID: {user_id}\n"
                f"👤 Имя: {user_data.get('fio', 'Неизвестно')}\n"
                f"📞 Телефон: {user_data.get('phone', 'Не указан')}\n\n"
                f"Для подключения нажмите на кнопку:"
            )

            # Создаем кнопку для начала диалога
            buttons_payload = ButtonsPayload(buttons=[[
                CallbackButton(
                    text="Начать диалог",
                    payload=f"start_chat:{user_id}"
                )
            ]])
            keyboard = Attachment(
                type=AttachmentType.INLINE_KEYBOARD,
                payload=buttons_payload
            )

            await bot.send_message(
                chat_id=admin_chat_id,
                text=message,
                attachments=[keyboard]
            )

            log_system_event("support_chat", "admin_notified", user_id=user_id, admin_chat_id=admin_chat_id)

        except Exception as e:
            log_system_event("support_chat", "notify_admin_error", error=str(e))

    async def connect_admin_to_chat(self, bot, admin_id: int, user_id: int, admin_chat_id: int = None) -> bool:
        """Подключает администратора к чату с пользователем"""
        success_message_sent = False
        try:
            # Получаем chat_id для админа если он не передан
            target_admin_chat_id = admin_chat_id
            if not target_admin_chat_id:
                target_admin_chat_id = db.get_last_chat_id(admin_id)
            
            if not target_admin_chat_id:
                log_system_event("support_chat", "admin_chat_id_not_found", admin_id=admin_id)
                return False

            # Проверяем права админа
            if admin_id != self.admin_id:
                await bot.send_message(
                    chat_id=target_admin_chat_id,
                    text="❌ У вас нет права администратора."
                )
                return False

            # Проверяем, не занят ли уже админ
            if self.admin_active_chat is not None and self.admin_active_chat != user_id:
                await bot.send_message(
                    chat_id=target_admin_chat_id,
                    text=f"❌ У вас уже есть активный чат с пользователем {self.admin_active_chat}.\n\nЗавершите его командой 0 или сообщением"
                )
                return False
            
            # Если админ уже в этом чате, просто говорим ему об этом (идемпотентность)
            if self.admin_active_chat == user_id:
                 await bot.send_message(
                    chat_id=target_admin_chat_id,
                    text=f"👨‍⚕️ Вы уже в диалоге с этим пользователем."
                )
                 return True

            # Проверяем, существует ли чат
            if user_id not in self.active_chats:
                await bot.send_message(
                    chat_id=target_admin_chat_id,
                    text=f"❌ Чат с пользователем {user_id} не найден или уже завершен."
                )
                return False

            # Проверяем, не подключен ли уже другой админ (или этот же)
            chat_info = self.active_chats[user_id]
            if not chat_info.get('waiting_for_admin', True):
                 if chat_info.get('admin_id') != admin_id:
                    await bot.send_message(
                        chat_id=admin_id,
                        text="❌ Этот чат уже обрабатывается другим оператором."
                    )
                    return False

            # Сохраняем target_admin_chat_id в self.chat_logs для будущего использования если нужно
            if user_id in self.chat_logs:
                 self.chat_logs[user_id].admin_id = admin_id

            # Подключаем админа
            self.admin_active_chat = user_id
            chat_info['waiting_for_admin'] = False
            chat_info['admin_id'] = admin_id
            chat_info['last_activity'] = time.time()

            # Отправляем подтверждение админу
            await bot.send_message(
                chat_id=target_admin_chat_id,
                text=f"✅ Вы начал диалог с пользователем {user_id}.\n\nВсе ваши текстовые сообщения будут пересылаться ему.\n\nДля завершения чата отправьте цифру 0."
            )
            success_message_sent = True

            # Отправляем накопленные сообщения от пользователя админу
            messages_queue = chat_info.get('messages_queue', [])
            if messages_queue:
                try:
                    await bot.send_message(
                        chat_id=target_admin_chat_id,
                        text=f"📨 Сообщения от пользователя (отправлены до вашего подключения):"
                    )

                    for msg in messages_queue:
                        try:
                            # Поддерживаем как старый формат (строка), так и новый (словарь)
                            if isinstance(msg, dict):
                                message_text = msg.get('text', '[Изображение]')
                                image_url = msg.get('image_url')
                            else:
                                message_text = msg
                                image_url = None
                            
                            message_text_to_send = f"👤 Пользователь: {message_text}"
                            
                            # Формируем attachments для пересылки изображения
                            message_attachments = []
                            if image_url:
                                try:
                                    image_attachment = Attachment(
                                        type=AttachmentType.IMAGE,
                                        payload=OtherAttachmentPayload(url=image_url)
                                    )
                                    message_attachments.append(image_attachment)
                                except Exception as e:
                                    log_system_event("support_chat", "create_image_attachment_error",
                                                   error=str(e), user_id=user_id)
                                    message_text_to_send += f"\n\n📷 Изображение: {image_url}"
                            
                            await bot.send_message(
                                chat_id=target_admin_chat_id,
                                text=message_text_to_send,
                                attachments=message_attachments if message_attachments else []
                            )
                        except Exception as e:
                            log_system_event("support_chat", "send_queued_message_error",
                                           error=str(e), user_id=user_id, admin_id=admin_id)
                            # Продолжаем отправку остальных сообщений
                except Exception as e:
                    log_system_event("support_chat", "send_queue_header_error",
                                   error=str(e), user_id=user_id, admin_id=admin_id)
                    # Продолжаем выполнение, даже если не удалось отправить заголовок очереди

            # Очищаем очередь
            chat_info['messages_queue'] = []

            log_user_event(str(user_id), "admin_connected", admin_id=admin_id)
            log_security_event(str(admin_id), "chat_started", target_user_id=user_id)

            return True
        except Exception as e:
            import traceback
            error_traceback = traceback.format_exc()
            log_system_event("support_chat", "connect_admin_error",
                           error=str(e), user_id=user_id, admin_id=admin_id, 
                           traceback=error_traceback)
            # Если успешное сообщение уже отправлено, не отправляем сообщение об ошибке
            if not success_message_sent:
                try:
                    target_id = target_admin_chat_id if 'target_admin_chat_id' in locals() and target_admin_chat_id else admin_id
                    await bot.send_message(
                        chat_id=target_id,
                        text=f"❌ Произошла ошибка при подключении к чату с пользователем {user_id}."
                    )
                except:
                    pass
            # Если успешное сообщение отправлено, но произошла ошибка позже, 
            # все равно возвращаем True, так как подключение фактически состоялось
            return success_message_sent

    def _extract_image_url(self, attachments: Optional[List[Attachment]]) -> Optional[str]:
        """Извлекает URL изображения из attachments"""
        if not attachments:
            return None
        
        for attachment in attachments:
            # Проверяем тип attachment
            if hasattr(attachment, 'type'):
                attachment_type = attachment.type
                # Может быть строкой "image" или AttachmentType.IMAGE
                is_image = (
                    attachment_type == "image" or 
                    str(attachment_type).lower() == "image" or
                    (hasattr(AttachmentType, 'IMAGE') and attachment_type == AttachmentType.IMAGE)
                )
                
                if is_image:
                    # Извлекаем URL из payload
                    if hasattr(attachment, 'payload'):
                        payload = attachment.payload
                        # Проверяем разные варианты payload
                        if hasattr(payload, 'url'):
                            return payload.url
                        elif isinstance(payload, dict):
                            return payload.get('url') or payload.get('token')
                        elif hasattr(payload, 'token'):
                            # Если есть токен, можно использовать его для получения URL
                            token = getattr(payload, 'token', None)
                            if token:
                                # Формируем URL из токена (если нужно)
                                return token
                    # Если payload нет или не удалось извлечь, пробуем получить напрямую
                    if hasattr(attachment, 'url'):
                        return attachment.url
        return None

    async def process_user_message(self, bot, user_id: int, message_text: str, attachments: Optional[List[Attachment]] = None) -> bool:
        """Обрабатывает сообщение от пользователя в чате"""
        # Проверяем команду выхода
        if message_text.strip() == "0":
            await self._end_chat(bot, user_id, "user", "user_exit")
            return True

        # Проверяем, в чате ли пользователь
        if user_id not in self.active_chats:
            return False

        chat_info = self.active_chats[user_id]
        chat_info['last_activity'] = time.time()

        # Извлекаем URL изображения, если есть
        image_url = self._extract_image_url(attachments)

        # Добавляем сообщение в лог
        if user_id in self.chat_logs:
            self.chat_logs[user_id].messages.append(
                ChatMessage(
                    from_user="user",
                    text=message_text or "[Изображение]",
                    time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    image_url=image_url
                )
            )
            # Обновляем один и тот же файл при каждом сообщении
            self._save_chat_log(user_id)

        # Если админ еще не подключен, сохраняем в очередь
        if chat_info.get('waiting_for_admin', True):
            messages_queue = chat_info.get('messages_queue', [])
            queue_item = {
                'text': message_text,
                'image_url': image_url
            }
            messages_queue.append(queue_item)
            chat_info['messages_queue'] = messages_queue
            return True

        # Если админ подключен - пересылаем сообщение
        admin_id = chat_info.get('admin_id')
        if admin_id:
            try:
                # Формируем текст сообщения
                message_text_to_send = f"👤 Пользователь {user_id}:\n{message_text}" if message_text else f"👤 Пользователь {user_id} отправил изображение"
                
                # Формируем attachments для пересылки изображения
                message_attachments = []
                if image_url:
                    try:
                        image_attachment = Attachment(
                            type=AttachmentType.IMAGE,
                            payload=OtherAttachmentPayload(url=image_url)
                        )
                        message_attachments.append(image_attachment)
                    except Exception as e:
                        log_system_event("support_chat", "create_image_attachment_error",
                                       error=str(e), user_id=user_id)
                        # Если не удалось создать attachment, просто добавим URL в текст
                        message_text_to_send += f"\n\n📷 Изображение: {image_url}"
                
                # Получаем chat_id администратора
                target_admin_chat_id = db.get_last_chat_id(admin_id)
                if not target_admin_chat_id:
                    log_system_event("support_chat", "admin_chat_id_not_found_for_forwarding", admin_id=admin_id)
                    return True # Сообщение не доставлено, но считаем обработанным во избежание ретраев

                await bot.send_message(
                    chat_id=target_admin_chat_id,
                    text=message_text_to_send,
                    attachments=message_attachments if message_attachments else []
                )
                return True
            except Exception as e:
                log_system_event("support_chat", "forward_to_admin_error",
                                 error=str(e), user_id=user_id)

        return False

    async def process_admin_message(self, bot, admin_id: int, message_text: str, attachments: Optional[List[Attachment]] = None) -> bool:
        """Обрабатывает сообщение от администратора"""
        try:
            # Проверяем права
            if admin_id != self.admin_id:
                return False

            # Проверяем команду выхода
            if message_text and message_text.strip() == "0":
                if self.admin_active_chat:
                    await self._end_chat(bot, self.admin_active_chat, "admin", "admin_exit")
                else:
                    await bot.send_message(
                        chat_id=admin_id,
                        text="❌ У вас нет активного чата для завершения."
                    )
                return True

            # Если админ не в чате...
            if not self.admin_active_chat:
                return False

            user_id = self.admin_active_chat

            # Проверяем, существует ли чат
            if user_id not in self.active_chats:
                await bot.send_message(
                    chat_id=admin_id,
                    text="❌ Чат с пользователем не найден или уже завершен."
                )
                self.admin_active_chat = None
                return False

            chat_info = self.active_chats[user_id]
            chat_info['last_activity'] = time.time()

            # Извлекаем URL изображения, если есть
            image_url = self._extract_image_url(attachments)

            # Добавляем сообщение в лог
            if user_id in self.chat_logs:
                self.chat_logs[user_id].messages.append(
                    ChatMessage(
                        from_user="admin",
                        text=message_text or "[Изображение]",
                        time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        image_url=image_url
                    )
                )
                # Обновляем один и тот же файл при каждом сообщении
                self._save_chat_log(user_id)

            # Пересылаем сообщение пользователю
            # Пересылаем сообщение пользователю
            try:
                target_chat_id = chat_info.get('chat_id')
                if not target_chat_id:
                     # Если вдруг chat_id нет (редко), пробуем взять user_id, но лучше бы из БД
                     target_chat_id = user_id
                     log_system_event("support_chat", "missing_chat_id_in_active", user_id=user_id)

                # Формируем текст сообщения
                message_text_to_send = f"👨‍⚕️ Оператор:\n{message_text}" if message_text else "👨‍⚕️ Оператор отправил изображение"
                
                # Формируем attachments для пересылки изображения
                message_attachments = []
                if image_url:
                    try:
                        image_attachment = Attachment(
                            type=AttachmentType.IMAGE,
                            payload=OtherAttachmentPayload(url=image_url)
                        )
                        message_attachments.append(image_attachment)
                    except Exception as e:
                        log_system_event("support_chat", "create_image_attachment_error",
                                       error=str(e), user_id=user_id)
                        # Если не удалось создать attachment, просто добавим URL в текст
                        message_text_to_send += f"\n\n📷 Изображение: {image_url}"
                
                await bot.send_message(
                    chat_id=target_chat_id,
                    text=message_text_to_send,
                    attachments=message_attachments if message_attachments else []
                )
                return True
            except Exception as e:
                log_system_event("support_chat", "forward_to_user_error",
                                 error=str(e), user_id=user_id)
                await bot.send_message(
                    chat_id=admin_id,
                    text=f"❌ Не удалось отправить сообщение пользователю {user_id}."
                )
                return False
        except Exception as e:
            # Логируем ошибку, но не пробрасываем её дальше, чтобы не показывать пользователю
            log_system_event("support_chat", "process_admin_message_error",
                           error=str(e), admin_id=admin_id, message=message_text)
            # Возвращаем True, чтобы остановить дальнейшую обработку сообщения
            return True

    async def _end_chat(self, bot, user_id: int, ended_by: str, reason: str):
        """Завершает чат"""
        chat_info = self.active_chats.get(user_id)
        if not chat_info:
            return

        # Сохраняем информацию перед очисткой структур
        admin_id = chat_info.get('admin_id')

        # Отправляем уведомления напрямую через бота (до очистки структур)
        try:
            target_chat_id = chat_info.get('chat_id') if chat_info else user_id
            
            if ended_by == "user":
                # Пользователю
                try:
                    # Если завершил пользователь, ему можно подтвердить, но необязательно, 
                    # если он сам нажал "выйти". Но оставим для ясности.
                    if target_chat_id:
                        await bot.send_message(
                            chat_id=target_chat_id,
                            text="Чат с техподдержкой завершён."
                        )
                except Exception as e:
                    log_system_event("support_chat", "end_notification_user_error",
                                     error=str(e), user_id=user_id)

                # Админу (если подключен или если еще не подключился - уведомляем через self.admin_id)
                target_admin_id = admin_id if admin_id else self.admin_id
                if target_admin_id:
                    try:
                        # Получаем chat_id администратора
                        target_admin_chat_id = db.get_last_chat_id(target_admin_id)
                        if target_admin_chat_id:
                            await bot.send_message(
                                chat_id=target_admin_chat_id,
                                text=f"Пользователь {user_id} завершил чат."
                            )
                    except Exception as e:
                        log_system_event("support_chat", "end_notification_admin_error",
                                         error=str(e), admin_id=target_admin_id, user_id=user_id)

            elif ended_by == "admin":
                # Пользователю
                try:
                    if target_chat_id:
                        await bot.send_message(
                            chat_id=target_chat_id,
                            text="Оператор завершил чат."
                        )
                except Exception as e:
                    log_system_event("support_chat", "end_notification_user_error",
                                     error=str(e), user_id=user_id)

                # Админу (подтверждение)
                if admin_id:
                    try:
                        # Получаем chat_id администратора
                        target_admin_chat_id = db.get_last_chat_id(admin_id)
                        if target_admin_chat_id:
                            await bot.send_message(
                                chat_id=target_admin_chat_id,
                                text=f"Чат с пользователем {user_id} завершён."
                            )
                    except Exception as e:
                        log_system_event("support_chat", "end_notification_admin_error",
                                         error=str(e), admin_id=admin_id, user_id=user_id)

            elif ended_by == "system":
                # Пользователю
                try:
                    if target_chat_id:
                        await bot.send_message(
                            chat_id=target_chat_id,
                            text="Чат автоматически завершен из-за неактивности."
                        )
                except Exception as e:
                    log_system_event("support_chat", "end_notification_user_error",
                                     error=str(e), user_id=user_id)

                # Админу (если подключен)
                if admin_id:
                    try:
                        await bot.send_message(
                            chat_id=admin_id,
                            text=f"Чат с пользователем {user_id} автоматически завершен."
                        )
                    except Exception as e:
                        log_system_event("support_chat", "end_notification_admin_error",
                                         error=str(e), admin_id=admin_id, user_id=user_id)
        except Exception as e:
            log_system_event("support_chat", "end_chat_notifications_error",
                             error=str(e), user_id=user_id)

        # Сохраняем лог
        self._save_chat_log(user_id, end_chat=True)

        # Очищаем структуры
        if user_id in self.active_chats:
            del self.active_chats[user_id]

        if self.admin_active_chat == user_id:
            self.admin_active_chat = None

        # Проверяем очередь ожидания
        await self._check_waiting_queue()

        log_user_event(str(user_id), "chat_ended", ended_by=ended_by, reason=reason)

    async def _send_message_to_user(self, user_id: int, message: str):
        """Отправляет сообщение пользователю (через бота)"""
        # Эта функция будет вызываться из bot.py
        # Сохраняем сообщение для отправки
        if user_id not in self.active_chats:
            self.active_chats[user_id] = {}

        self.active_chats[user_id]['pending_notification'] = message

    async def _send_message_to_admin(self, admin_id: int, message: str):
        """Отправляет сообщение админу (через бота)"""
        # Эта функция будет вызываться из bot.py
        # Сохраняем сообщение для отправки
        if 'admin_notifications' not in self.__dict__:
            self.admin_notifications = {}

        self.admin_notifications[admin_id] = message

    async def _check_waiting_queue(self):
        """Проверяет очередь ожидания и уведомляет админа о следующем пользователе"""
        if not self.waiting_queue or self.admin_active_chat is not None:
            return

        # Берем первого пользователя из очереди
        next_chat = self.waiting_queue.pop(0)
        user_id = next_chat['user_id']
        chat_id = next_chat['chat_id']
        user_data = next_chat['user_data']

        # Создаем новый чат
        self._create_new_chat(user_id, chat_id, user_data)

        bot = self._get_bot()
        if bot:
            # Сообщаем пользователю, что оператор освободился
            await bot.send_message(
                chat_id=chat_id,
                text="⏳ Оператор освободился. Опишите вашу проблему или вопрос. Как только оператор подключится, он увидит все ваши сообщения.\n\nЧтобы выйти из чата — отправьте цифру 0."
            )
            # Уведомляем админа о следующем пользователе в очереди
            if self.admin_id:
                await self._notify_admin_new_chat(bot, user_id, chat_id, user_data)

        log_system_event("support_chat", "next_user_notified", user_id=user_id)

    def set_bot(self, bot):
        """Устанавливает экземпляр бота для отправки сообщений"""
        self._bot_instance = bot

    def _get_bot(self):
        """Получает экземпляр бота"""
        return getattr(self, '_bot_instance', None)


# Глобальный экземпляр
support_handler = None


def init_support_handler(user_states_dict):
    """Инициализация поддержки после создания user_states"""
    global support_handler
    support_handler = SupportHandler(user_states_dict)
    return support_handler
