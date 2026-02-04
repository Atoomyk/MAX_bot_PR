# sync_appointments/service.py
"""
Основной сервис синхронизации записей к врачу.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime, date, timedelta

from .fetcher import Fetcher
from .parser import Parser
from .matcher import Matcher
from .database import AppointmentsDatabase
from .notifier import Notifier
from .cancel_service import CancelService

logger = logging.getLogger(__name__)


class SyncService:
    """
    Основной сервис для координации всей синхронизации.
    """

    def __init__(self, user_database, bot_instance, mis_url: str = None):
        """
        Инициализация сервиса синхронизации.

        Args:
            user_database: База данных пользователей
            bot_instance: Экземпляр бота MAX API
            mis_url: URL внешней системы МИС
        """
        self.user_db = user_database
        self.bot = bot_instance
        self.mis_url = mis_url

        # Инициализация компонентов
        self.fetcher = Fetcher(base_url=mis_url)
        self.parser = Parser()
        self.matcher = Matcher(user_database)
        self.appointments_db = AppointmentsDatabase(user_database)
        self.notifier = Notifier(bot_instance, self.appointments_db, user_database)
        self.cancel_service = CancelService()

        self.last_sync_time = None
        self.last_sync_result = None

    async def run_sync(self) -> Dict[str, Any]:
        """
        Запускает полный процесс синхронизации.

        Returns:
            Словарь с результатами синхронизации
        """
        logger.info("=" * 60)
        logger.info("НАЧАЛО СИНХРОНИЗАЦИИ ЗАПИСЕЙ К ВРАЧУ")
        logger.info("=" * 60)

        start_time = datetime.now()

        try:
            # 1. Получение данных из внешней системы
            logger.info("1. Получение данных из внешней системы МИС...")
            raw_data = await self.fetcher.fetch_data()

            if raw_data is None:
                error_msg = "Не удалось получить данные из внешней системы"
                logger.error(error_msg)
                return self._create_error_result(error_msg, start_time)

            # 2. Парсинг данных
            logger.info("2. Парсинг полученных данных...")
            parsed_records = self.parser.parse_response(raw_data)

            if not parsed_records:
                logger.warning("Нет данных для обработки после парсинга")
                return self._create_success_result([], start_time, 0, 0, 0)

            # 3. Сопоставление пациентов с пользователями бота
            logger.info("3. Сопоставление пациентов с пользователями бота...")
            matching_results = self.matcher.batch_match(parsed_records)

            matched_records = matching_results['matched']
            unmatched_records = matching_results['unmatched']

            if not matched_records:
                logger.warning("Не найдено сопоставлений с пользователями бота")
                return self._create_success_result([], start_time,
                                                   len(parsed_records), 0, 0)

            # 4. Сохранение записей в БД и сбор новых записей для уведомлений
            logger.info("4. Сохранение записей в базу данных...")
            user_new_appointments = {}
            added_appointment_ids = set() # ID только что добавленных записей
            total_saved = 0
            skipped_reminders_off = 0
            skipped_already_exists = 0

            for match in matched_records:
                user_id = match['user_id']
                patient_data = match['patient_data']

                # Получаем данные записи
                appointment_data = patient_data['appointment_data']
                metadata = patient_data['metadata']
                visit_time = metadata['visit_time']
                mo_name = metadata['mo_name']

                logger.info(f"Обработка записи для user_id={user_id}, время={visit_time}, МО={mo_name}")

                # Проверяем, включены ли уведомления у пользователя
                reminders_status = self.matcher.get_user_reminders_status(user_id)
                if not reminders_status:
                    logger.warning(f"Уведомления отключены для пользователя {user_id}, запись НЕ БУДЕТ сохранена")
                    skipped_reminders_off += 1
                    continue

                logger.debug(f"Уведомления включены для пользователя {user_id}")

                # Сохраняем запись в БД
                save_result = self.appointments_db.add_appointment(
                    user_id=user_id,
                    appointment_data=appointment_data,
                    visit_time=visit_time,
                    mo_name=mo_name
                )

                if not save_result.get('success'):
                    logger.warning(f"✗ Не удалось сохранить запись для user_id={user_id}: {save_result.get('error')}")
                    continue

                db_id = save_result.get('id') or self._get_last_inserted_id(
                    user_id,
                    visit_time,
                    mo_name,
                    book_id_mis=appointment_data.get('Book_Id_Mis')
                )

                if save_result.get('inserted'):
                    logger.info(f"✓ Запись успешно сохранена для user_id={user_id}")
                    total_saved += 1
                    if db_id:
                        added_appointment_ids.add(db_id)
                else:
                    skipped_already_exists += 1

                # --- Отправка "напоминания" один раз за сутки до приема ---
                # Даже если запись уже была в БД (например, создана самим ботом),
                # уведомление должно прийти один раз, если еще не отправлялось.
                if db_id:
                    already_sent = self.appointments_db.get_reminder_24h_sent_at(db_id) is not None
                    if not already_sent:
                        if user_id not in user_new_appointments:
                            user_new_appointments[user_id] = []

                        user_new_appointments[user_id].append({
                            'db_id': db_id,
                            'matching_data': patient_data.get('matching_data', {}),
                            'appointment_data': appointment_data,
                            'metadata': metadata,
                            'patient_fio': patient_data.get('matching_data', {}).get('full_fio', 'не указано'),
                            'visit_time': visit_time,
                            'mo_name': mo_name,
                            'mo_address': appointment_data.get('Адрес мед учреждения', 'не указано'),
                            'doctor_fio': appointment_data.get('ФИО врача', 'не указано'),
                            'doctor_position': appointment_data.get('Должность врача', 'не указано')
                        })

            # 4.1. Проверка отмененных записей (которые есть в БД, но нет в ответе МИС)
            logger.info("4.1. Проверка удаленных в МИС записей...")
            
            # Собираем множества всех полученных записей:
            # 1) Основное: (user_id, book_id_mis) — самый надежный ключ
            # 2) Fallback: (user_id, visit_time, mo_name) — если book_id_mis отсутствует
            rmis_book_ids_set = set()
            rmis_appointments_set = set()
            for match in matched_records:
                u_id = match['user_id']
                p_data = match['patient_data']
                m_data = p_data.get('metadata', {})
                a_data = p_data.get('appointment_data', {}) or {}

                book_id_mis = a_data.get('Book_Id_Mis')
                if book_id_mis:
                    rmis_book_ids_set.add((u_id, str(book_id_mis)))
                
                # Приводим дату к строке без микросекунд для надежного сравнения
                # Если в БД timestamp без зоны, а в парсере naive datetime - должно совпасть
                visit_time = m_data['visit_time']
                if isinstance(visit_time, str):
                    # Если вдруг строка (хотя парсер возвращает datetime)
                    visit_time_str = visit_time
                else:
                    # Округляем до секунд (убираем микросекунды) и приводим к ISO
                    visit_time_str = visit_time.replace(microsecond=0).isoformat()

                rmis_appointments_set.add((u_id, visit_time_str, m_data['mo_name']))
            
            # Получаем все активные будущие записи из БД
            active_appointments = self.appointments_db.get_all_active_future_appointments()
            
            cancelled_by_sync_count = 0
            
            tomorrow = date.today() + timedelta(days=1)

            for local_app in active_appointments:
                app_id = local_app['id']
                
                # Если мы только что добавили эту запись, не нужно её проверять/отменять
                if app_id in added_appointment_ids:
                    continue

                # Проверяем «удалено в МИС» только для записей на завтра:
                # парсер возвращает данные только на завтра, для других дат данных из МИС нет
                local_visit_time = local_app.get('visit_time')
                if local_visit_time:
                    visit_date = (
                        local_visit_time.date()
                        if hasattr(local_visit_time, 'date')
                        else None
                    )
                    if visit_date is None and isinstance(local_visit_time, str):
                        try:
                            parsed = datetime.fromisoformat(
                                local_visit_time.replace('Z', '+00:00')
                            )
                            visit_date = parsed.date()
                        except (ValueError, TypeError):
                            visit_date = None
                    if visit_date is not None and visit_date != tomorrow:
                        continue

                local_book_id_mis = local_app.get('book_id_mis')

                # Основной путь: сравнение по Book_Id_Mis
                if local_book_id_mis:
                    local_key = (local_app['user_id'], str(local_book_id_mis))
                    missing_in_rmis = local_key not in rmis_book_ids_set
                    debug_keys_sample = list(rmis_book_ids_set)[:3]
                else:
                    # Fallback: сравнение по (время, МО)
                    local_visit_time = local_app['visit_time']
                    if isinstance(local_visit_time, str):
                        local_visit_time_str = local_visit_time
                    elif local_visit_time:
                        local_visit_time_str = local_visit_time.replace(microsecond=0).isoformat()
                    else:
                        continue  # Некорректная запись в БД

                    local_key = (local_app['user_id'], local_visit_time_str, local_app['mo_name'])
                    missing_in_rmis = local_key not in rmis_appointments_set
                    debug_keys_sample = list(rmis_appointments_set)[:3]

                # Если локальной записи нет в множестве записей из МИС -> она удалена
                if missing_in_rmis:
                    u_id = local_app['user_id']
                    
                    logger.warning(f"Обнаружена удаленная в МИС запись: id={app_id}, user={u_id}")
                    # Логируем детали для отладки (повышаем уровень до WARNING, чтобы видеть в консоли юзера)
                    logger.warning(f"  > Ключ локальный: {local_key}")
                    logger.warning(f"  > Ключи МИС (первые 3): {debug_keys_sample}")
                    
                    # Отменяем локально
                    cancel_result = self.appointments_db.cancel_appointment(
                        appointment_id=app_id,
                        user_id=u_id,
                        cancelled_by='system_sync',
                        force=True # Игнорируем проверку времени
                    )
                    
                    if cancel_result['success']:
                        logger.info(f"Запись {app_id} отменена системой синхронизации")
                        cancelled_by_sync_count += 1

            # 5. Отправка уведомлений пользователям
            logger.info("5. Отправка уведомлений пользователям...")
            notification_results = None

            if user_new_appointments:
                # Конвертируем user_id из str в int для MAX API
                user_appointments_int = {}
                for user_id_str, appointments in user_new_appointments.items():
                    try:
                        user_id_int = int(user_id_str)
                        user_appointments_int[user_id_int] = appointments
                    except ValueError:
                        logger.error(f"Некорректный user_id: {user_id_str}")
                        continue

                notification_results = await self.notifier.send_batch_notifications(user_appointments_int)

                # Если уведомление реально отправлено — проставляем reminder_24h_sent_at
                try:
                    details = (notification_results or {}).get('details', {}) or {}
                    for uid_str, status in details.items():
                        if status != 'sent':
                            continue
                        try:
                            uid_int = int(uid_str)
                        except ValueError:
                            continue
                        apps = user_appointments_int.get(uid_int, [])
                        ids_to_mark = [a.get('db_id') for a in apps if a.get('db_id')]
                        if ids_to_mark:
                            self.appointments_db.mark_reminder_24h_sent(ids_to_mark)
                except Exception as e:
                    logger.warning(f"Не удалось отметить отправленные напоминания: {e}")
            else:
                logger.info("Нет новых записей для уведомлений")

            # 6. Формирование итогового результата
            logger.info("6. Формирование отчета о синхронизации...")
            result = self._create_success_result(
                parsed_records=parsed_records,
                start_time=start_time,
                total_parsed=len(parsed_records),
                total_matched=len(matched_records),
                total_saved=total_saved,
                unmatched_count=len(unmatched_records),
                notification_results=notification_results
            )

            self.last_sync_time = datetime.now()
            self.last_sync_result = result

            logger.info("=" * 60)
            logger.info("СИНХРОНИЗАЦИЯ УСПЕШНО ЗАВЕРШЕНА")
            logger.info("=" * 60)

            return result

        except Exception as e:
            error_msg = f"Критическая ошибка синхронизации: {e}"
            logger.error(error_msg, exc_info=True)
            return self._create_error_result(error_msg, start_time)

    def _get_last_inserted_id(self, user_id: str, visit_time: datetime, mo_name: str, book_id_mis: Optional[str] = None) -> Optional[int]:
        """
        Получает ID последней вставленной записи.

        Args:
            user_id: ID пользователя
            visit_time: Время приема
            mo_name: Название мед учреждения

        Returns:
            ID записи или None
        """
        try:
            if book_id_mis:
                query = """
                SELECT id FROM appointments
                WHERE user_id = %s
                  AND book_id_mis = %s
                  AND status = 'active'
                ORDER BY created_at DESC
                LIMIT 1
                """
                self.appointments_db.cursor.execute(query, (user_id, str(book_id_mis)))
            else:
                query = """
                SELECT id FROM appointments 
                WHERE user_id = %s 
                AND external_visit_time = %s 
                AND external_mo_name = %s
                AND status = 'active'
                ORDER BY created_at DESC
                LIMIT 1
                """
                self.appointments_db.cursor.execute(query, (user_id, visit_time, mo_name))
            row = self.appointments_db.cursor.fetchone()
            return row[0] if row else None
        except Exception as e:
            logger.error(f"Ошибка получения ID записи: {e}")
            return None

    def _create_success_result(self, parsed_records, start_time,
                               total_parsed, total_matched, total_saved,
                               unmatched_count=0, notification_results=None) -> Dict[str, Any]:
        """
        Создает словарь с результатами успешной синхронизации.
        """
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        parser_stats = self.parser.get_stats()
        matcher_stats = self.matcher.get_stats()
        notifier_stats = self.notifier.get_stats() if notification_results else {}

        result = {
            'success': True,
            'timestamp': end_time.isoformat(),
            'duration_seconds': duration,
            'summary': {
                'total_received': total_parsed,
                'successfully_parsed': parser_stats.get('processed', 0),
                'parse_errors': parser_stats.get('errors', 0),
                'patients_matched': total_matched,
                'patients_unmatched': unmatched_count,
                'new_appointments_saved': total_saved,
                'match_rate_percent': matcher_stats.get('match_rate', 0),
                'parse_success_rate_percent': parser_stats.get('success_rate', 0)
            },
            'notifications': notifier_stats,
            'components': {
                'parser': parser_stats,
                'matcher': matcher_stats,
                'notifier': notifier_stats
            }
        }

        logger.info(f"РЕЗУЛЬТАТЫ СИНХРОНИЗАЦИИ:")
        logger.info(f"  • Получено записей: {total_parsed}")
        logger.info(f"  • Успешно распаршено: {parser_stats.get('processed', 0)}")
        logger.info(f"  • Найдено пациентов: {total_matched}")
        logger.info(f"  • Сохранено новых записей: {total_saved}")
        logger.info(f"  • Уведомлений отправлено: {notifier_stats.get('sent', 0)}")
        logger.info(f"  • Время выполнения: {duration:.2f} сек")

        return result

    def _create_error_result(self, error_message: str, start_time: datetime) -> Dict[str, Any]:
        """
        Создает словарь с результатами при ошибке синхронизации.
        """
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        result = {
            'success': False,
            'timestamp': end_time.isoformat(),
            'duration_seconds': duration,
            'error': error_message,
            'summary': {
                'total_received': 0,
                'successfully_parsed': 0,
                'parse_errors': 0,
                'patients_matched': 0,
                'patients_unmatched': 0,
                'new_appointments_saved': 0
            }
        }

        return result

    async def run_cleanup(self, days_to_keep: int = 365) -> Dict[str, Any]:
        """
        Запускает очистку старых записей.

        Args:
            days_to_keep: Хранить записи не старше этого количества дней

        Returns:
            Результаты очистки
        """
        logger.info("Запуск очистки старых записей...")
        start_time = datetime.now()

        try:
            deleted_count = self.appointments_db.cleanup_old_appointments(days_to_keep)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result = {
                'success': True,
                'timestamp': end_time.isoformat(),
                'duration_seconds': duration,
                'deleted_count': deleted_count,
                'days_to_keep': days_to_keep
            }

            logger.info(f"Очистка завершена. Удалено записей: {deleted_count}")
            return result

        except Exception as e:
            logger.error(f"Ошибка очистки старых записей: {e}")

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            return {
                'success': False,
                'timestamp': end_time.isoformat(),
                'duration_seconds': duration,
                'error': str(e),
                'deleted_count': 0
            }

    async def health_check(self) -> Dict[str, Any]:
        """
        Проверяет состояние всех компонентов системы.

        Returns:
            Словарь с результатами проверки
        """
        checks = {
            'external_api': False,
            'database': False,
            'bot_connection': False,
            'overall': False
        }

        try:
            # Проверка внешнего API
            checks['external_api'] = await self.fetcher.health_check()

            # Проверка базы данных
            checks['database'] = self._check_database_connection()

            # Проверка соединения с ботом (упрощенно)
            checks['bot_connection'] = True  # В реальном коде нужно проверить

            # Общая проверка
            checks['overall'] = all([
                checks['external_api'],
                checks['database'],
                checks['bot_connection']
            ])

            logger.debug(f"Health check results: {checks}")

        except Exception as e:
            logger.error(f"Ошибка health check: {e}")

        return checks

    def _check_database_connection(self) -> bool:
        """
        Проверяет подключение к базе данных.
        """
        try:
            if not self.user_db.conn:
                return False

            self.user_db.cursor.execute("SELECT 1")
            return True
        except Exception:
            return False

    def get_status(self) -> Dict[str, Any]:
        """
        Возвращает текущий статус сервиса синхронизации.

        Returns:
            Словарь со статусом
        """
        stats = self.appointments_db.get_stats()

        status = {
            'last_sync_time': self.last_sync_time.isoformat() if self.last_sync_time else None,
            'last_sync_success': self.last_sync_result.get('success') if self.last_sync_result else None,
            'database_stats': stats,
            'components_status': {
                'fetcher': self.fetcher.get_request_info() if hasattr(self.fetcher, 'get_request_info') else {},
                'parser': self.parser.get_stats() if hasattr(self.parser, 'get_stats') else {},
                'matcher': self.matcher.get_stats() if hasattr(self.matcher, 'get_stats') else {},
                'notifier': self.notifier.get_stats() if hasattr(self.notifier, 'get_stats') else {}
            }
        }

        return status

    async def force_sync_with_mock(self, mock_file_path: str) -> Dict[str, Any]:
        """
        Запускает синхронизацию с использованием мок-данных из файла.

        Args:
            mock_file_path: Путь к файлу с тестовыми данными

        Returns:
            Результаты синхронизации
        """
        logger.info(f"Запуск синхронизации с мок-данными из {mock_file_path}")
        start_time = datetime.now()

        try:
            # 1. Читаем файл напрямую
            import json
            import os

            if not os.path.exists(mock_file_path):
                error_msg = f"Файл не найден: {mock_file_path}"
                logger.error(error_msg)
                return self._create_error_result(error_msg, datetime.now())

            with open(mock_file_path, 'r', encoding='utf-8') as f:
                mock_data = json.load(f)

            logger.info(f"Загружено {len(mock_data.get('InformerResult', []))} записей из мок-файла")

            # 2. Парсинг данных (прямо в сервисе)
            logger.info("Парсинг мок-данных...")
            parsed_records = self.parser.parse_response(mock_data)

            if not parsed_records:
                logger.warning("Нет данных для обработки после парсинга мок-файла")
                return self._create_success_result([], datetime.now(), 0, 0, 0)

            # 3. Сопоставление пациентов с пользователями бота
            logger.info("Сопоставление пациентов с пользователями бота...")
            matching_results = self.matcher.batch_match(parsed_records)

            matched_records = matching_results['matched']
            unmatched_records = matching_results['unmatched']

            if not matched_records:
                logger.warning("Не найдено сопоставлений с пользователями бота")
                return self._create_success_result(parsed_records, datetime.now(),
                                                   len(parsed_records), 0, 0)

            # 4. Сохранение записей в БД и сбор новых записей для уведомлений
            logger.info("4. Сохранение записей в базу данных...")
            user_appointments = {}
            total_saved = 0

            for match in matched_records:
                user_id = match['user_id']
                patient_data = match['patient_data']

                # Проверяем, включены ли уведомления у пользователя
                if not self.matcher.get_user_reminders_status(user_id):
                    logger.debug(f"Уведомления отключены для пользователя {user_id}, пропускаем")
                    continue

                # Получаем данные записи
                appointment_data = patient_data['appointment_data']
                metadata = patient_data['metadata']

                visit_time = metadata['visit_time']
                mo_name = metadata['mo_name']

                # Сохраняем запись в БД
                save_result = self.appointments_db.add_appointment(
                    user_id=user_id,
                    appointment_data=appointment_data,
                    visit_time=visit_time,
                    mo_name=mo_name
                )

                if not save_result.get('success'):
                    continue

                if save_result.get('inserted'):
                    total_saved += 1

                    # Получаем ID сохраненной записи для кнопки отмены
                    db_id = save_result.get('id') or self._get_last_inserted_id(
                        user_id,
                        visit_time,
                        mo_name,
                        book_id_mis=appointment_data.get('Book_Id_Mis')
                    )

                    # Подготавливаем данные для уведомления
                    if user_id not in user_appointments:
                        user_appointments[user_id] = []

                    # Получаем ВСЕ данные для уведомления
                    user_appointments[user_id].append({
                        # ID записи в БД для кнопки отмены
                        'db_id': db_id,
                        # Данные для отображения
                        'matching_data': patient_data.get('matching_data', {}),
                        'appointment_data': appointment_data,
                        'metadata': metadata,
                        # Добавляем оригинальные поля для простоты доступа
                        'patient_fio': patient_data.get('matching_data', {}).get('full_fio', 'не указано'),
                        'visit_time': visit_time,
                        'mo_name': mo_name,
                        'mo_address': appointment_data.get('Адрес мед учреждения', 'не указано'),
                        'doctor_fio': appointment_data.get('ФИО врача', 'не указано'),
                        'doctor_position': appointment_data.get('Должность врача', 'не указано')
                    })

            # 5. Отправка уведомлений пользователям
            logger.info("5. Отправка уведомлений пользователям...")
            notification_results = None

            if user_appointments:
                # Конвертируем user_id из str в int для MAX API
                user_appointments_int = {}
                for user_id_str, appointments in user_appointments.items():
                    try:
                        user_id_int = int(user_id_str)
                        user_appointments_int[user_id_int] = appointments
                    except ValueError:
                        logger.error(f"Некорректный user_id: {user_id_str}")
                        continue

                notification_results = await self.notifier.send_batch_notifications(user_appointments_int)
            else:
                logger.info("Нет пользователей для уведомлений")

            # 6. Формируем результат
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            parser_stats = self.parser.get_stats()
            matcher_stats = self.matcher.get_stats()
            notifier_stats = self.notifier.get_stats() if notification_results else {}

            result = {
                'success': True,
                'timestamp': end_time.isoformat(),
                'duration_seconds': duration,
                'summary': {
                    'total_received': len(parsed_records),
                    'successfully_parsed': parser_stats.get('processed', 0),
                    'parse_errors': parser_stats.get('errors', 0),
                    'patients_matched': len(matched_records),
                    'patients_unmatched': len(unmatched_records),
                    'new_appointments_saved': total_saved,
                },
                'notifications': notifier_stats
            }

            logger.info(f"МОК-СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА:")
            logger.info(f"  • Обработано записей: {len(parsed_records)}")
            logger.info(f"  • Найдено пациентов: {len(matched_records)}")
            logger.info(f"  • Сохранено записей в БД: {total_saved}")
            logger.info(f"  • Отправлено уведомлений: {notifier_stats.get('sent', 0)}")

            return result

        except json.JSONDecodeError as e:
            error_msg = f"Ошибка чтения JSON файла: {e}"
            logger.error(error_msg)
            return self._create_error_result(error_msg, datetime.now())

        except Exception as e:
            logger.error(f"Ошибка синхронизации с мок-данными: {e}", exc_info=True)
            return self._create_error_result(str(e), datetime.now())