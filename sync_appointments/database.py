# sync_appointments/database.py
"""
Работа с таблицей appointments в базе данных.
"""

import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import psycopg2

logger = logging.getLogger(__name__)


class AppointmentsDatabase:
    """
    Класс для работы с таблицей записей к врачу.
    """

    def __init__(self, db_connection):
        """
        Инициализация с подключением к БД.

        Args:
            db_connection: Объект подключения к PostgreSQL
        """
        self.conn = db_connection.conn
        self.cursor = db_connection.cursor
        self._init_appointments_table()

    def _init_appointments_table(self) -> None:
        """
        Создание таблицы appointments и необходимых индексов.
        """
        if not self.conn:
            logger.error("Нет подключения к базе данных")
            return

        try:
            # Создание таблицы appointments
            create_table_query = """
            CREATE TABLE IF NOT EXISTS appointments (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                appointment_json JSONB NOT NULL,
                external_visit_time TIMESTAMP,
                external_mo_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'active',
                cancelled_at TIMESTAMP NULL,

                CONSTRAINT fk_user
                    FOREIGN KEY (user_id) 
                    REFERENCES users(user_id)
                    ON DELETE CASCADE
            );
            """
            self.cursor.execute(create_table_query)
            
            # Миграция: добавляем поля status и cancelled_at если их нет
            self._add_column_if_not_exists('status', "VARCHAR(20) DEFAULT 'active'")
            self._add_column_if_not_exists('cancelled_at', 'TIMESTAMP NULL')
            self._add_column_if_not_exists('cancelled_by', 'VARCHAR(50) NULL')
            self._add_column_if_not_exists('book_id_mis', 'TEXT NULL')
            self._add_column_if_not_exists('reminder_24h_sent_at', 'TIMESTAMP NULL')
            
            # Обновляем существующие записи: устанавливаем status = 'active'
            try:
                self.cursor.execute("""
                    UPDATE appointments 
                    SET status = 'active' 
                    WHERE status IS NULL
                """)
                self.conn.commit()
            except Exception as e:
                logger.warning(f"Не удалось обновить существующие записи: {e}")
                if self.conn:
                    self.conn.rollback()

            # Создание индексов
            # ВАЖНО: ранее idx_appointments_user_visit_mo был UNIQUE, что приводило к блокировке сохранения
            # при наличии записи, созданной самим ботом, даже если Book_Id_Mis отличается.
            # Сейчас уникальность обеспечивается (user_id, book_id_mis), поэтому этот индекс делаем не-уникальным.
            try:
                self.cursor.execute(
                    """
                    SELECT indexdef
                    FROM pg_indexes
                    WHERE tablename = 'appointments'
                      AND indexname = 'idx_appointments_user_visit_mo'
                    """
                )
                row = self.cursor.fetchone()
                if row and row[0] and 'UNIQUE' in row[0].upper():
                    self.cursor.execute("DROP INDEX IF EXISTS idx_appointments_user_visit_mo")
                    self.conn.commit()
            except Exception as e:
                logger.warning(f"Не удалось проверить/удалить UNIQUE индекс idx_appointments_user_visit_mo: {e}")
                if self.conn:
                    self.conn.rollback()

            indexes = [
                ("idx_appointments_user_id", "appointments (user_id)"),
                ("idx_appointments_user_visit_mo",
                 "appointments (user_id, external_visit_time, external_mo_name)"),
                ("idx_appointments_visit_time", "appointments (external_visit_time)"),
                ("idx_appointments_created_at", "appointments (created_at)"),
                ("idx_appointments_status", "appointments (user_id, status)")
            ]

            for index_name, index_def, *unique in indexes:
                try:
                    unique_clause = "UNIQUE" if unique and unique[0] else ""
                    self.cursor.execute(f"""
                        CREATE {unique_clause} INDEX IF NOT EXISTS {index_name}
                        ON {index_def};
                    """)
                except Exception as e:
                    logger.warning(f"Не удалось создать индекс {index_name}: {e}")

            self.conn.commit()
            logger.info("Таблица appointments проверена/создана")

        except Exception as e:
            logger.error(f"Ошибка инициализации таблицы appointments: {e}")
            if self.conn:
                self.conn.rollback()

    def appointment_exists(self, user_id: int, visit_time: datetime, mo_name: str) -> bool:
        """
        Проверяет, существует ли уже запись с такими же данными.

        Args:
            user_id: ID пользователя (int)
            visit_time: Дата и время приема
            mo_name: Название мед учреждения

        Returns:
            True если запись уже существует
        """
        try:
            query = """
            SELECT 1 FROM appointments 
            WHERE user_id = %s 
            AND external_visit_time = %s 
            AND external_mo_name = %s
            LIMIT 1
            """
            self.cursor.execute(query, (user_id, visit_time, mo_name))
            return self.cursor.fetchone() is not None

        except Exception as e:
            logger.error(f"Ошибка проверки существования записи: {e}")
            return False

    def appointment_exists_by_book_id_mis(self, user_id: int, book_id_mis: str) -> bool:
        """
        Проверяет, существует ли уже запись с таким же Book_Id_Mis.
        Это главный ключ дедупликации для записей из МИС.
        """
        try:
            if not book_id_mis:
                return False
            query = """
            SELECT 1 FROM appointments
            WHERE user_id = %s
              AND book_id_mis = %s
              AND status = 'active'
            LIMIT 1
            """
            self.cursor.execute(query, (user_id, book_id_mis))
            return self.cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Ошибка проверки существования записи по book_id_mis: {e}")
            return False

    def add_appointment(self, user_id: int, appointment_data: Dict[str, Any],
                        visit_time: datetime, mo_name: str) -> Dict[str, Any]:
        """
        Добавляет новую запись к врачу.
        ВАЖНО: Проверяет существование пользователя в таблице users перед вставкой.

        Args:
            user_id: ID пользователя (int)
            appointment_data: Данные о записи в формате словаря
            visit_time: Дата и время приема
            mo_name: Название мед учреждения

        Returns:
            dict:
            - success: bool
            - inserted: bool (True если создана новая строка, False если обновили существующую)
            - id: Optional[int] (id строки в БД, если смогли получить)
        """
        try:
            # Проверка существования пользователя в users
            self.cursor.execute("SELECT 1 FROM users WHERE user_id = %s", (user_id,))
            if not self.cursor.fetchone():
                logger.warning(f"Пропуск добавления записи: пользователь user_id={user_id} не найден в базе")
                return {"success": False, "inserted": False, "id": None}  # пользователь не зарегистрирован
            
            # Преобразуем данные в JSON
            appointment_json = json.dumps(appointment_data, ensure_ascii=False)

            book_id_mis = appointment_data.get("Book_Id_Mis")
            if isinstance(book_id_mis, str):
                book_id_mis = book_id_mis.strip() or None

            # Если есть Book_Id_Mis — это главный ключ уникальности (upsert)
            if book_id_mis:
                try:
                    query = """
                    INSERT INTO appointments
                        (user_id, appointment_json, book_id_mis, external_visit_time, external_mo_name, status)
                    VALUES
                        (%s, %s, %s, %s, %s, 'active')
                    ON CONFLICT (user_id, book_id_mis)
                    DO UPDATE SET
                        appointment_json = COALESCE(appointments.appointment_json, '{}'::jsonb) || EXCLUDED.appointment_json,
                        external_visit_time = COALESCE(EXCLUDED.external_visit_time, appointments.external_visit_time),
                        external_mo_name = COALESCE(EXCLUDED.external_mo_name, appointments.external_mo_name),
                        status = 'active',
                        cancelled_at = NULL
                    RETURNING (xmax = 0) AS inserted, id
                    """
                    self.cursor.execute(query, (user_id, appointment_json, book_id_mis, visit_time, mo_name))
                    row = self.cursor.fetchone()
                    self.conn.commit()
                    inserted = bool(row[0]) if row else False
                    row_id = int(row[1]) if row and row[1] is not None else None
                    if inserted:
                        logger.info(f"Добавлена новая запись для user_id={user_id}, book_id_mis={book_id_mis}")
                    else:
                        logger.debug(f"Запись обновлена/уже существовала для user_id={user_id}, book_id_mis={book_id_mis}")
                    return {"success": True, "inserted": inserted, "id": row_id}
                except psycopg2.IntegrityError as e:
                    # Частый кейс: в БД уже есть запись с тем же (user_id, external_visit_time, external_mo_name),
                    # но пришел другой/обновленный Book_Id_Mis из МИС.
                    # Уникальный индекс idx_appointments_user_visit_mo блокирует вставку -> мерджим в существующую строку.
                    constraint = getattr(getattr(e, "diag", None), "constraint_name", None)
                    pgcode = getattr(e, "pgcode", None)
                    if pgcode == "23505" and constraint == "idx_appointments_user_visit_mo":
                        if self.conn:
                            self.conn.rollback()
                        try:
                            self.cursor.execute(
                                """
                                SELECT id, book_id_mis
                                FROM appointments
                                WHERE user_id = %s
                                  AND external_visit_time = %s
                                  AND external_mo_name = %s
                                LIMIT 1
                                """,
                                (user_id, visit_time, mo_name),
                            )
                            row = self.cursor.fetchone()
                            if not row:
                                return {"success": False, "inserted": False, "id": None, "error": str(e)}

                            existing_id = int(row[0])
                            existing_book_id = row[1]

                            # Если Book_Id_Mis отличается — сохраняем старый в JSON для отладки
                            merge_patch = dict(appointment_data)
                            if existing_book_id and str(existing_book_id) != str(book_id_mis):
                                merge_patch.setdefault("Book_Id_Mis_Original", str(existing_book_id))

                            patch_json = json.dumps(merge_patch, ensure_ascii=False)

                            # Обновляем существующую строку и проставляем актуальный book_id_mis
                            self.cursor.execute(
                                """
                                UPDATE appointments
                                SET appointment_json = COALESCE(appointment_json, '{}'::jsonb) || %s::jsonb,
                                    book_id_mis = %s,
                                    status = 'active',
                                    cancelled_at = NULL
                                WHERE id = %s
                                RETURNING id
                                """,
                                (patch_json, str(book_id_mis), existing_id),
                            )
                            upd = self.cursor.fetchone()
                            self.conn.commit()
                            updated_id = int(upd[0]) if upd and upd[0] is not None else existing_id
                            logger.info(
                                "Сопоставили запись по (user_id, visit_time, mo_name) и обновили book_id_mis: user_id=%s, id=%s",
                                user_id,
                                updated_id,
                            )
                            return {"success": True, "inserted": False, "id": updated_id}
                        except Exception as inner:
                            logger.error(f"Ошибка мерджа записи при конфликте idx_appointments_user_visit_mo: {inner}")
                            if self.conn:
                                self.conn.rollback()
                            return {"success": False, "inserted": False, "id": None, "error": str(inner)}

                    # Любая другая integrity-ошибка
                    if self.conn:
                        self.conn.rollback()
                    return {"success": False, "inserted": False, "id": None, "error": str(e)}

            # Fallback: старый ключ (встречается если МИС не прислала Book_Id_Mis)
            query = """
            INSERT INTO appointments 
                (user_id, appointment_json, external_visit_time, external_mo_name, status)
            VALUES
                (%s, %s, %s, %s, 'active')
            ON CONFLICT (user_id, external_visit_time, external_mo_name) 
            DO NOTHING
            RETURNING id
            """
            self.cursor.execute(query, (user_id, appointment_json, visit_time, mo_name))
            row = self.cursor.fetchone()
            self.conn.commit()
            if row:
                row_id = int(row[0])
                logger.info(f"Добавлена новая запись для user_id={user_id}, время={visit_time}")
                return {"success": True, "inserted": True, "id": row_id}
            logger.debug(f"Запись уже существует для user_id={user_id}, время={visit_time}")
            return {"success": True, "inserted": False, "id": None}

        except Exception as e:
            logger.error(f"Ошибка добавления записи: {e}")
            if self.conn:
                self.conn.rollback()
            return {"success": False, "inserted": False, "id": None, "error": str(e)}

    def get_reminder_24h_sent_at(self, appointment_id: int) -> Optional[datetime]:
        try:
            self.cursor.execute(
                "SELECT reminder_24h_sent_at FROM appointments WHERE id = %s",
                (appointment_id,)
            )
            row = self.cursor.fetchone()
            return row[0] if row else None
        except Exception as e:
            logger.warning(f"Не удалось получить reminder_24h_sent_at для записи {appointment_id}: {e}")
            return None

    def mark_reminder_24h_sent(self, appointment_ids: List[int]) -> int:
        """
        Проставляет reminder_24h_sent_at = NOW() для набора записей (если еще не проставлено).
        Возвращает количество обновленных строк.
        """
        if not appointment_ids:
            return 0
        try:
            self.cursor.execute(
                """
                UPDATE appointments
                SET reminder_24h_sent_at = NOW()
                WHERE id = ANY(%s)
                  AND reminder_24h_sent_at IS NULL
                """,
                (appointment_ids,),
            )
            updated = self.cursor.rowcount or 0
            self.conn.commit()
            return updated
        except Exception as e:
            logger.warning(f"Не удалось проставить reminder_24h_sent_at: {e}")
            if self.conn:
                self.conn.rollback()
            return 0

    def get_user_appointments(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Получает список записей пользователя.

        Args:
            user_id: ID пользователя (int)
            limit: Максимальное количество записей

        Returns:
            Список записей пользователя
        """
        try:
            query = """
            SELECT id, appointment_json, external_visit_time, external_mo_name, created_at, status
            FROM appointments 
            WHERE user_id = %s AND status = 'active'
            ORDER BY external_visit_time DESC
            LIMIT %s
            """

            self.cursor.execute(query, (user_id, limit))
            rows = self.cursor.fetchall()

            appointments = []
            for row in rows:
                try:
                    appointment_data = json.loads(row[1])  # appointment_json
                    appointments.append({
                        'id': row[0],
                        'data': appointment_data,
                        'visit_time': row[2],
                        'mo_name': row[3],
                        'created_at': row[4],
                        'status': row[5] if len(row) > 5 else 'active'
                    })
                except json.JSONDecodeError as e:
                    logger.error(f"Ошибка парсинга JSON записи id={row[0]}: {e}")

            return appointments

        except Exception as e:
            logger.error(f"Ошибка получения записей пользователя {user_id}: {e}")
            return []

    def get_all_active_future_appointments(self) -> List[Dict[str, Any]]:
        """
        Получает все активные будущие записи для всех пользователей.
        Используется для синхронизации (поиск удаленных в МИС записей).
        """
        try:
            query = """
            SELECT id, user_id, book_id_mis, external_visit_time, external_mo_name
            FROM appointments 
            WHERE status = 'active' AND external_visit_time >= NOW()
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            appointments = []
            for row in rows:
                appointments.append({
                    'id': row[0],
                    'user_id': row[1],
                    'book_id_mis': row[2],
                    'visit_time': row[3],
                    'mo_name': row[4]
                })
            return appointments
            
        except Exception as e:
            logger.error(f"Ошибка получения всех активных записей: {e}")
            return []

    def get_appointment_by_id(self, appointment_id: int, user_id: int = None) -> Optional[Dict[str, Any]]:
        """
        Получает запись по ID.

        Args:
            appointment_id: ID записи
            user_id: Опционально - ID пользователя для проверки принадлежности

        Returns:
            Данные записи или None
        """
        try:
            if user_id:
                query = """
                SELECT appointment_json FROM appointments 
                WHERE id = %s AND user_id = %s
                """
                params = (appointment_id, user_id)
            else:
                query = "SELECT appointment_json FROM appointments WHERE id = %s"
                params = (appointment_id,)

            self.cursor.execute(query, params)
            row = self.cursor.fetchone()

            if row:
                return json.loads(row[0])
            return None

        except Exception as e:
            logger.error(f"Ошибка получения записи id={appointment_id}: {e}")
            return None

    def cleanup_old_appointments(self, days_to_keep: int = 365) -> int:
        """
        Удаляет записи старше указанного количества дней.

        Args:
            days_to_keep: Хранить записи не старше этого количества дней

        Returns:
            Количество удаленных записей
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)

            query = """
            DELETE FROM appointments 
            WHERE external_visit_time < %s 
            RETURNING id
            """

            self.cursor.execute(query, (cutoff_date,))
            deleted_count = self.cursor.rowcount
            self.conn.commit()

            logger.info(f"Удалено {deleted_count} записей старше {days_to_keep} дней")
            return deleted_count

        except Exception as e:
            logger.error(f"Ошибка очистки старых записей: {e}")
            if self.conn:
                self.conn.rollback()
            return 0

    def get_stats(self) -> Dict[str, Any]:
        """
        Получает статистику по записям.

        Returns:
            Словарь со статистикой
        """
        try:
            stats = {}

            # Общее количество записей
            self.cursor.execute("SELECT COUNT(*) FROM appointments")
            stats['total_appointments'] = self.cursor.fetchone()[0] or 0

            # Количество уникальных пользователей с записями
            self.cursor.execute("SELECT COUNT(DISTINCT user_id) FROM appointments")
            stats['unique_users'] = self.cursor.fetchone()[0] or 0

            # Последняя добавленная запись
            self.cursor.execute("""
                SELECT MAX(created_at) FROM appointments
            """)
            stats['last_sync'] = self.cursor.fetchone()[0]

            # Записи по дням (последние 7 дней)
            self.cursor.execute("""
                SELECT DATE(created_at) as day, COUNT(*) 
                FROM appointments 
                WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY DATE(created_at)
                ORDER BY day DESC
            """)
            stats['last_7_days'] = dict(self.cursor.fetchall())

            return stats

        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {}

    def _add_column_if_not_exists(self, column_name: str, column_definition: str) -> None:
        """
        Добавляет колонку в таблицу, если она не существует.

        Args:
            column_name: Имя колонки
            column_definition: Определение колонки (тип и ограничения)
        """
        try:
            # Проверяем, существует ли колонка
            self.cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='appointments' AND column_name=%s
            """, (column_name,))
            
            if not self.cursor.fetchone():
                # Колонка не существует, добавляем её
                self.cursor.execute(f"""
                    ALTER TABLE appointments 
                    ADD COLUMN {column_name} {column_definition}
                """)
                self.conn.commit()
                logger.info(f"Добавлена колонка {column_name} в таблицу appointments")
        except Exception as e:
            logger.warning(f"Не удалось добавить колонку {column_name}: {e}")
            if self.conn:
                self.conn.rollback()

    def cancel_appointment(self, appointment_id: int, user_id: int, cancelled_by: str = 'user_cancel', force: bool = False) -> Dict[str, Any]:
        """
        Отменяет запись к врачу.

        Args:
            appointment_id: ID записи
            user_id: ID пользователя (int)
            cancelled_by: Кто отменил ('user_cancel', 'system_sync')
            force: Если True, игнорировать проверки времени (для системной синхронизации)

        Returns:
            Словарь с результатом:
            - success: bool - успешность операции
            - error: str - сообщение об ошибке (если есть)
            - appointment_data: dict - данные записи (если успешно)
        """
        try:
            # Получаем запись с проверкой принадлежности и статуса
            query = """
            SELECT id, appointment_json, created_at, status, cancelled_at
            FROM appointments 
            WHERE id = %s AND user_id = %s
            """
            self.cursor.execute(query, (appointment_id, user_id))
            row = self.cursor.fetchone()

            if not row:
                return {
                    'success': False,
                    'error': 'Запись не найдена или не принадлежит вам'
                }

            appointment_id_db, appointment_json, created_at, status, cancelled_at = row

            # Проверяем статус
            if status == 'cancelled':
                return {
                    'success': False,
                    'error': 'Запись уже отменена'
                }

            # Проверяем, прошло ли более 3 часов с момента создания (если не force)
            if not force and created_at:
                time_diff = datetime.now() - created_at
                if time_diff.total_seconds() > 3 * 3600:  # 3 часа в секундах
                    return {
                        'success': False,
                        'error': 'Нельзя отменить запись, если прошло более 3 часов с момента создания'
                    }

            # Обновляем запись
            update_query = """
            UPDATE appointments 
            SET status = 'cancelled', cancelled_at = CURRENT_TIMESTAMP, cancelled_by = %s
            WHERE id = %s AND user_id = %s AND status = 'active'
            RETURNING appointment_json
            """
            self.cursor.execute(update_query, (cancelled_by, appointment_id, user_id))
            
            if self.cursor.rowcount == 0:
                return {
                    'success': False,
                    'error': 'Не удалось отменить запись (возможно, она уже отменена)'
                }

            self.conn.commit()
            
            # appointment_json может быть уже словарем (JSONB) или строкой
            appointment_json_result = self.cursor.fetchone()[0]
            if isinstance(appointment_json_result, str):
                appointment_data = json.loads(appointment_json_result)
            elif isinstance(appointment_json_result, dict):
                # Уже словарь, ничего не делаем
                appointment_data = appointment_json_result
            else:
                # Пытаемся преобразовать в строку и распарсить
                appointment_data = json.loads(str(appointment_json_result))
            
            logger.info(f"Запись {appointment_id} успешно отменена пользователем {user_id}, кем: {cancelled_by}")
            
            return {
                'success': True,
                'appointment_data': appointment_data
            }

        except Exception as e:
            logger.error(f"Ошибка отмены записи {appointment_id} для пользователя {user_id}: {e}")
            if self.conn:
                self.conn.rollback()
            return {
                'success': False,
                'error': f'Ошибка при отмене записи: {str(e)}'
            }

    def get_appointment_by_id_with_status(self, appointment_id: int, user_id: int = None) -> Optional[Dict[str, Any]]:
        """
        Получает запись по ID с информацией о статусе.

        Args:
            appointment_id: ID записи
            user_id: Опционально - ID пользователя для проверки принадлежности

        Returns:
            Словарь с данными записи и статусом или None
        """
        try:
            if user_id:
                query = """
                SELECT id, appointment_json, status, cancelled_at, created_at, cancelled_by
                FROM appointments 
                WHERE id = %s AND user_id = %s
                """
                params = (appointment_id, user_id)
            else:
                query = """
                SELECT id, appointment_json, status, cancelled_at, created_at, cancelled_by
                FROM appointments 
                WHERE id = %s
                """
                params = (appointment_id,)

            self.cursor.execute(query, params)
            row = self.cursor.fetchone()

            if row:
                # appointment_json может быть уже словарем (JSONB) или строкой
                appointment_data = row[1]
                if isinstance(appointment_data, str):
                    appointment_data = json.loads(appointment_data)
                elif isinstance(appointment_data, dict):
                    # Уже словарь, ничего не делаем
                    pass
                else:
                    # Пытаемся преобразовать в строку и распарсить
                    appointment_data = json.loads(str(appointment_data))
                
                # Безопасно получаем cancelled_by, если столбца/данных еще нет в старых записях
                cancelled_by = row[5] if len(row) > 5 else None

                return {
                    'id': row[0],
                    'data': appointment_data,
                    'status': row[2],
                    'cancelled_at': row[3],
                    'created_at': row[4],
                    'cancelled_by': cancelled_by
                }
            return None

        except Exception as e:
            logger.error(f"Ошибка получения записи id={appointment_id}: {e}")
            return None