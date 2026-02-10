import os
import re
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
from logging_config import log_system_event

load_dotenv()

# --- Конфигурация PostgreSQL ---
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")


class UserDatabase:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self._connect()
        self._init_db()
        self._create_reminders_table()  # ← создаём таблицу напоминаний
        self._create_mvp_tables()       # ← создаём таблицы для MVP функционала (подписание, телемед, направления, записи)

    # ---------------------------------------------------------------------
    # Подключение
    # ---------------------------------------------------------------------
    def _connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
            )
            self.cursor = self.conn.cursor()
            log_system_event("database", "user_db_connected")
        except psycopg2.Error as e:
            log_system_event("database", "user_db_connection_failed", error=str(e))

    # ---------------------------------------------------------------------
    # Инициализация таблицы users
    # ---------------------------------------------------------------------
    def _init_db(self):
        if not self.conn:
            return

        try:
            # ДРОПАЕМ старые таблицы удалены, теперь только CREATE IF NOT EXISTS
            # self.cursor.execute("DROP TABLE IF EXISTS user_reminders CASCADE;")
            # self.cursor.execute("DROP TABLE IF EXISTS users CASCADE;")
            
            create_table_query = """
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                last_chat_id BIGINT,
                fio TEXT NOT NULL,
                phone VARCHAR(20) UNIQUE NOT NULL,
                birth_date VARCHAR(10) NOT NULL,
                snils VARCHAR(14),
                oms VARCHAR(16),
                gender VARCHAR(10),
                registration_date TEXT NOT NULL
            );
            """
            self.cursor.execute(create_table_query)

            self.conn.commit()
            log_system_event("database", "users_table_initialized")
        except psycopg2.Error as e:
            log_system_event("database", "users_table_init_error", error=str(e))
            if self.conn:
                self.conn.rollback()

    # ---------------------------------------------------------------------
    # Создание таблицы user_reminders
    # ---------------------------------------------------------------------
    def _create_reminders_table(self):
        """
        Создаёт таблицу напоминаний, если не существует.
        """
        try:
            query = """
            CREATE TABLE IF NOT EXISTS user_reminders (
                id SERIAL PRIMARY KEY,
                user_id BIGINT UNIQUE NOT NULL,
                enabled BOOLEAN NOT NULL DEFAULT TRUE,
                updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                
                CONSTRAINT fk_user_reminders_user
                    FOREIGN KEY (user_id) 
                    REFERENCES users(user_id)
                    ON DELETE CASCADE
            );
            """
            self.cursor.execute(query)
            self.conn.commit()
            log_system_event("database", "reminders_table_initialized")
        except psycopg2.Error as e:
            log_system_event("database", "reminders_table_init_error", error=str(e))
            self.conn.rollback()

    # ---------------------------------------------------------------------
    # Создание таблиц для MVP (подписание, телемед, направления, записи)
    # ---------------------------------------------------------------------
    def _create_mvp_tables(self):
        """
        Создаёт таблицы для новых функций MVP с запасом полей.
        1. signing_requests (Подписание через Госключ/Госуслуги)
        2. telemed_sessions (Телемедицина)
        3. referrals (Направления)
        4. appointments (История и будущие записи)
        """
        if not self.conn:
            return

        queries = [
            # 1. Таблица для подписания документов
            """
            CREATE TABLE IF NOT EXISTS signing_requests (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                request_type VARCHAR(50),       -- Тип запроса (contract, consent, etc.)
                doc_title TEXT,                 -- Название документа
                doc_url TEXT,                   -- Ссылка на документ
                signing_method VARCHAR(50),     -- goskey / gosuslugi / simple_sms
                status VARCHAR(50) DEFAULT 'pending', 
                external_id VARCHAR(100),       -- ID во внешней системе
                created_at TIMESTAMP DEFAULT NOW(),
                signed_at TIMESTAMP,
                
                -- Поля с запасом (Reserve)
                metadata TEXT,                  -- JSON строка для доп. данных
                error_message TEXT,             -- Сообщение об ошибке, если сорвалось
                ip_address VARCHAR(50),         -- IP пользователя
                user_agent TEXT,                -- Устройство пользователя
                
                CONSTRAINT fk_signing_user 
                    FOREIGN KEY (user_id) 
                    REFERENCES users(user_id)
            );
            """,
            
            # 2. Таблица для телемедицинских консультаций (интеграция с МИС)
            """
            CREATE TABLE IF NOT EXISTS telemed_sessions (
                -- Основные идентификаторы
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                external_id VARCHAR(255) UNIQUE NOT NULL,
                
                -- Данные пациента
                user_id BIGINT REFERENCES users(user_id) ON DELETE SET NULL,
                patient_phone VARCHAR(20) NOT NULL,
                patient_fio VARCHAR(255) NOT NULL,
                patient_snils VARCHAR(14),
                patient_oms_number VARCHAR(16),
                patient_oms_series VARCHAR(10),
                patient_birth_date DATE,
                patient_sex VARCHAR(1),
                
                -- Данные врача
                doctor_fio VARCHAR(255) NOT NULL,
                doctor_snils VARCHAR(14),
                doctor_specialization VARCHAR(255),
                doctor_position VARCHAR(255),
                
                -- Данные клиники
                clinic_name VARCHAR(255),
                clinic_address VARCHAR(500),
                clinic_mo_oid VARCHAR(255),
                clinic_phone VARCHAR(20),
                
                -- Данные консультации
                schedule_date TIMESTAMPTZ NOT NULL,
                status VARCHAR(20) NOT NULL,
                pay_method VARCHAR(20),
                
                -- MAX чат
                chat_id BIGINT,
                chat_invite_link TEXT,
                
                -- Напоминания
                reminder_24h_at TIMESTAMPTZ,
                reminder_15m_at TIMESTAMPTZ,
                reminder_24h_sent_at TIMESTAMPTZ,
                reminder_15m_sent_at TIMESTAMPTZ,
                
                -- Согласие пациента
                consent_at TIMESTAMPTZ,
                consent_message_id BIGINT,
                
                -- Метаданные
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            """,
            
            # 2.1 Индексы для telemed_sessions (создаются в tmk/database.py, здесь пропускаем)
            # Индексы создаются автоматически при инициализации ТМК модуля
            # Если создать их здесь, будет ошибка если таблица ещё не существует

            # 3. Таблица направлений
            """
            CREATE TABLE IF NOT EXISTS referrals (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                referral_number VARCHAR(100),   -- Номер направления
                from_doctor TEXT,               -- Кто направил
                to_specialty TEXT,              -- К кому/куда
                target_mo TEXT,                 -- Целевая МО
                issue_date DATE,                -- Дата выдачи
                expiry_date DATE,               -- Срок действия
                status VARCHAR(50),             -- active, used, expired
                
                -- Поля с запасом
                diagnosis_code VARCHAR(20),     -- МКБ-10
                urgency VARCHAR(20),            -- cito / routine
                referral_doc_url TEXT,          -- Скан направления
                reason TEXT,                    -- Обоснование
                
                CONSTRAINT fk_referrals_user 
                    FOREIGN KEY (user_id) 
                    REFERENCES users(user_id)
            );
            """,

            # 4. Таблица всех записей (История + Будущие)
            # Структура согласно требованию: id, appointment_json, external_visit_time, external_mo_name, created_at, status, cancelled_at, user_id, booking_source (TEXT)
            """
            CREATE TABLE IF NOT EXISTS appointments (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                appointment_json JSONB,          -- Все данные записи в JSON
                external_visit_time TIMESTAMP,   -- Время визита (для удобства сортировки)
                external_mo_name TEXT,           -- Название МО (для удобства отображения)
                created_at TIMESTAMP DEFAULT NOW(),
                status VARCHAR(50),              -- active, cancelled, etc.
                cancelled_at TIMESTAMP,
                booking_source VARCHAR(20) DEFAULT 'self_bot',
                cancelled_by VARCHAR(50)         -- user_cancel / system_sync
            );
            """
        ]

        try:
            for q in queries:
                # Пропускаем пустые запросы
                if q and q.strip():
                    self.cursor.execute(q)
            self.conn.commit()
            
            # --- Миграция таблицы appointments ---
            
            self._add_column_if_not_exists("appointment_json", "JSONB", "appointments")
            self._add_column_if_not_exists("external_visit_time", "TIMESTAMP", "appointments")
            self._add_column_if_not_exists("external_mo_name", "TEXT", "appointments")
            self._add_column_if_not_exists("created_at", "TIMESTAMP DEFAULT NOW()", "appointments")
            self._add_column_if_not_exists("status", "VARCHAR(50)", "appointments")
            self._add_column_if_not_exists("cancelled_at", "TIMESTAMP", "appointments")
            
            # Обработка booking_source (переименование из registered_in_bot или visit_type)
            
            # 1. Проверяем registered_in_bot (из предыдущего шага)
            self.cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='appointments' AND column_name='registered_in_bot';")
            if self.cursor.fetchone():
                 self.cursor.execute("ALTER TABLE appointments RENAME COLUMN registered_in_bot TO booking_source;")
                 
                 # Миграция данных: 'myself' -> 'self_bot', 'other' -> 'other_bot'
                 self.cursor.execute("UPDATE appointments SET booking_source = 'self_bot' WHERE booking_source = 'myself';")
                 self.cursor.execute("UPDATE appointments SET booking_source = 'other_bot' WHERE booking_source = 'other';")
                 # Если были boolean (true/false) и не конвертнулись раньше:
                 # 'true' -> 'self_bot', 'false' -> 'other_bot' (на всякий случай)
                 
                 self.conn.commit()
                 log_system_event("database", "column_renamed", old="registered_in_bot", new="booking_source")
            
            # 2. Проверяем visit_type (совсем старое, если вдруг database откатили)
            self.cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='appointments' AND column_name='visit_type';")
            if self.cursor.fetchone():
                self.cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='appointments' AND column_name='booking_source';")
                if not self.cursor.fetchone(): # Если еще нет нового
                     self.cursor.execute("ALTER TABLE appointments RENAME COLUMN visit_type TO booking_source;")
                     self.conn.commit()

            # 3. Если ничего не было, создаем
            self._add_column_if_not_exists("booking_source", "VARCHAR(20) DEFAULT 'self_bot'", "appointments")

            # --- Миграция: book_id_mis для дедупликации/отмены ---
            self._migrate_appointments_book_id_mis()
            # --- Миграция: поле отправки напоминания (24ч) ---
            self._add_column_if_not_exists("reminder_24h_sent_at", "TIMESTAMP", "appointments")

            log_system_event("database", "mvp_tables_initialized")
        except psycopg2.Error as e:
            log_system_event("database", "mvp_tables_init_error", error=str(e))
            self.conn.rollback()

    def _migrate_appointments_book_id_mis(self):
        """
        Безопасная миграция:
        - добавляет колонку book_id_mis в appointments
        - бэкендит данные из appointment_json->>'Book_Id_Mis'
        - дедуплицирует записи по (user_id, book_id_mis) с бэкапом дублей
        - добавляет UNIQUE constraint на (user_id, book_id_mis)
        """
        if not self.conn:
            return

        try:
            # 1) Колонка book_id_mis
            self._add_column_if_not_exists("book_id_mis", "TEXT", "appointments")

            # 2) Нормализация пустых значений
            self.cursor.execute(
                """
                UPDATE appointments
                SET book_id_mis = NULL
                WHERE book_id_mis IS NOT NULL AND BTRIM(book_id_mis) = ''
                """
            )
            self.conn.commit()

            # 3) Backfill из JSON (если есть ключ Book_Id_Mis)
            self.cursor.execute(
                """
                UPDATE appointments
                SET book_id_mis = appointment_json->>'Book_Id_Mis'
                WHERE (book_id_mis IS NULL OR BTRIM(book_id_mis) = '')
                  AND appointment_json ? 'Book_Id_Mis'
                """
            )
            self.conn.commit()

            # 4) Повторная нормализация (на случай если JSON вернул пустую строку)
            self.cursor.execute(
                """
                UPDATE appointments
                SET book_id_mis = NULL
                WHERE book_id_mis IS NOT NULL AND BTRIM(book_id_mis) = ''
                """
            )
            self.conn.commit()

            # 5) Дедупликация по (user_id, book_id_mis) с бэкапом
            self._dedupe_appointments_by_book_id_mis()

            # 6) Уникальность (user_id, book_id_mis)
            self._ensure_unique_constraint_user_book_id_mis()

            log_system_event("database", "appointments_book_id_mis_migrated")
        except Exception as e:
            log_system_event("database", "appointments_book_id_mis_migration_failed", error=str(e))
            if self.conn:
                self.conn.rollback()

    def _ensure_unique_constraint_user_book_id_mis(self):
        """
        Добавляет UNIQUE constraint на (user_id, book_id_mis), если его ещё нет.
        PostgreSQL допускает несколько NULL, поэтому старые записи без book_id_mis не конфликтуют.
        """
        try:
            self.cursor.execute(
                """
                SELECT 1
                FROM information_schema.table_constraints
                WHERE table_name = 'appointments'
                  AND constraint_name = 'uniq_appointments_user_book_id_mis'
                LIMIT 1
                """
            )
            if self.cursor.fetchone():
                return

            # Перед добавлением констрейнта убеждаемся, что дублей по book_id_mis не осталось.
            # На всякий случай "разруливаем" остаточные дубли без удаления строк:
            # оставляем book_id_mis только на одной (самой новой) записи в группе.
            self.cursor.execute(
                """
                WITH ranked AS (
                    SELECT
                        id,
                        ROW_NUMBER() OVER (
                            PARTITION BY user_id, book_id_mis
                            ORDER BY created_at DESC NULLS LAST, id DESC
                        ) AS rn
                    FROM appointments
                    WHERE book_id_mis IS NOT NULL
                )
                UPDATE appointments a
                SET book_id_mis = NULL
                FROM ranked r
                WHERE a.id = r.id AND r.rn > 1
                """
            )
            self.conn.commit()

            self.cursor.execute(
                """
                ALTER TABLE appointments
                ADD CONSTRAINT uniq_appointments_user_book_id_mis
                UNIQUE (user_id, book_id_mis)
                """
            )
            self.conn.commit()
            log_system_event("database", "appointments_book_id_mis_unique_added")
        except Exception as e:
            log_system_event("database", "appointments_book_id_mis_unique_failed", error=str(e))
            if self.conn:
                self.conn.rollback()

    def _dedupe_appointments_by_book_id_mis(self):
        """
        Убирает дубли по (user_id, book_id_mis):
        - сохраняет все дубли в таблицу appointments_dedup_backup
        - мерджит appointment_json в одну "основную" запись
        - удаляет остальные строки-дубли
        """
        import json
        from datetime import datetime

        def deep_merge_keep_truthy(base, incoming):
            """Рекурсивный merge dict: заполняем пропуски, не затираем непустое."""
            if not isinstance(base, dict) or not isinstance(incoming, dict):
                return base if base not in (None, "", [], {}) else incoming
            out = dict(base)
            for k, v in incoming.items():
                if k not in out or out[k] in (None, "", [], {}):
                    out[k] = v
                    continue
                if isinstance(out[k], dict) and isinstance(v, dict):
                    out[k] = deep_merge_keep_truthy(out[k], v)
            return out

        try:
            self.cursor.execute(
                """
                SELECT user_id, book_id_mis,
                       array_agg(id ORDER BY created_at DESC NULLS LAST, id DESC) AS ids,
                       array_agg(appointment_json ORDER BY created_at DESC NULLS LAST, id DESC) AS jsons
                FROM appointments
                WHERE book_id_mis IS NOT NULL
                GROUP BY user_id, book_id_mis
                HAVING COUNT(*) > 1
                """
            )
            groups = self.cursor.fetchall()
            if not groups:
                return

            # Бэкап-таблица для дублей (без ограничений, чтобы можно было повторно запускать)
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS appointments_dedup_backup (
                    id BIGINT,
                    user_id BIGINT,
                    book_id_mis TEXT,
                    appointment_json JSONB,
                    external_visit_time TIMESTAMP,
                    external_mo_name TEXT,
                    created_at TIMESTAMP,
                    status VARCHAR(50),
                    cancelled_at TIMESTAMP,
                    booking_source VARCHAR(20),
                    cancelled_by VARCHAR(50),
                    backed_up_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """
            )
            self.conn.commit()

            for user_id, book_id_mis, ids, jsons in groups:
                if not ids or len(ids) < 2:
                    continue

                keep_id = ids[0]
                dup_ids = ids[1:]

                merged = {}
                for j in jsons:
                    if j is None:
                        continue
                    if isinstance(j, str):
                        try:
                            j = json.loads(j)
                        except Exception:
                            continue
                    if not isinstance(j, dict):
                        continue
                    merged = deep_merge_keep_truthy(merged, j)

                # Обогащаем/фиксируем ключи
                merged["Book_Id_Mis"] = book_id_mis

                # Пытаемся вычислить внешние поля
                mo_name = merged.get("Мед учреждение") or merged.get("external_mo_name") or None
                visit_raw = merged.get("Дата записи") or merged.get("external_visit_time") or None

                def parse_dt(s):
                    if not s:
                        return None
                    if isinstance(s, datetime):
                        return s.replace(tzinfo=None) if s.tzinfo else s
                    if not isinstance(s, str):
                        s = str(s)
                    s = s.strip()
                    if not s:
                        return None
                    try:
                        dt = datetime.fromisoformat(s)
                        return dt.replace(tzinfo=None) if dt.tzinfo else dt
                    except Exception:
                        return None

                visit_dt = parse_dt(visit_raw)

                # Бэкапим строки-дубли
                self.cursor.execute(
                    """
                    INSERT INTO appointments_dedup_backup (
                        id, user_id, book_id_mis, appointment_json,
                        external_visit_time, external_mo_name,
                        created_at, status, cancelled_at, booking_source, cancelled_by,
                        backed_up_at
                    )
                    SELECT
                        id, user_id, book_id_mis, appointment_json,
                        external_visit_time, external_mo_name,
                        created_at, status, cancelled_at, booking_source, cancelled_by,
                        NOW()
                    FROM appointments
                    WHERE id = ANY(%s)
                    """,
                    (dup_ids,)
                )

                # Обновляем основную запись (оставляем статус active)
                self.cursor.execute(
                    """
                    UPDATE appointments
                    SET appointment_json = %s,
                        book_id_mis = %s,
                        external_visit_time = COALESCE(%s, external_visit_time),
                        external_mo_name = COALESCE(%s, external_mo_name)
                    WHERE id = %s
                    """,
                    (json.dumps(merged, ensure_ascii=False), book_id_mis, visit_dt, mo_name, keep_id)
                )

                # Удаляем дубли
                self.cursor.execute("DELETE FROM appointments WHERE id = ANY(%s)", (dup_ids,))

            self.conn.commit()
            log_system_event("database", "appointments_book_id_mis_dedup_done", groups=len(groups))
        except Exception as e:
            log_system_event("database", "appointments_book_id_mis_dedup_failed", error=str(e))
            if self.conn:
                self.conn.rollback()

    # ---------------------------------------------------------------------
    # Создание записи для нового пользователя
    # ---------------------------------------------------------------------
    def init_user_reminder_record(self, user_id: int):
        """
        Создаёт запись с enabled=TRUE, если её еще нет.
        ВАЖНО: Проверяет существование пользователя в таблице users перед вставкой.
        """
        try:
            # Проверка существования пользователя в users
            self.cursor.execute(
                "SELECT 1 FROM users WHERE user_id = %s",
                (user_id,)
            )
            if not self.cursor.fetchone():
                log_system_event("database", "reminder_record_create_skipped", reason="user_not_found", user_id=user_id)
                return  # пользователь не зарегистрирован
            
            # Проверка существования записи о напоминаниях
            self.cursor.execute(
                "SELECT 1 FROM user_reminders WHERE user_id = %s",
                (user_id,)
            )
            if self.cursor.fetchone():
                return  # уже существует

            self.cursor.execute(
                """
                INSERT INTO user_reminders (user_id, enabled, updated_at)
                VALUES (%s, TRUE, NOW())
                """,
                (user_id,)
            )
            self.conn.commit()
            log_system_event("database", "reminder_record_created", user_id=user_id)

        except psycopg2.Error as e:
            log_system_event("database", "reminder_record_create_error", error=str(e), user_id=user_id)
            self.conn.rollback()

    # ---------------------------------------------------------------------
    # Получение полных данных пользователя для записи к врачу
    # ---------------------------------------------------------------------
    def get_user_full_data(self, user_id: int):
        """
        Возвращает dict {fio, birth_date, phone, snils, oms, gender} или None
        """
        try:
            self.cursor.execute(
                "SELECT fio, birth_date, phone, snils, oms, gender FROM users WHERE user_id = %s",
                (user_id,)
            )
            row = self.cursor.fetchone()
            if row:
                return {
                    'fio': row[0],
                    'birth_date': row[1],
                    'phone': row[2],
                    'snils': row[3],
                    'oms': row[4],
                    'gender': row[5]
                }
            return None
        except psycopg2.Error as e:
            log_system_event("database", "get_user_full_data_error", error=str(e), user_id=user_id)
            return None

    def get_user_by_phone(self, phone: str):
        """
        Поиск пользователя по номеру телефона (для ТМК)
        Возвращает dict {user_id, fio, phone} или None
        """
        try:
            self.cursor.execute(
                "SELECT user_id, fio, phone FROM users WHERE phone = %s",
                (phone,)
            )
            row = self.cursor.fetchone()
            if row:
                return {
                    'user_id': row[0],
                    'fio': row[1],
                    'phone': row[2]
                }
            return None
        except psycopg2.Error as e:
            log_system_event("database", "get_user_by_phone_error", error=str(e), phone=phone)
            return None

    # ---------------------------------------------------------------------
    # Получение статуса включено/выключено
    # ---------------------------------------------------------------------
    def get_reminders_status(self, user_id: int) -> bool:
        """
        Возвращает TRUE/FALSE.
        Если записи нет — создаёт по умолчанию TRUE (только для зарегистрированных пользователей).
        """
        try:
            # Проверка существования пользователя в users
            self.cursor.execute(
                "SELECT 1 FROM users WHERE user_id = %s",
                (user_id,)
            )
            if not self.cursor.fetchone():
                log_system_event("database", "get_reminders_status_skipped", reason="user_not_found", user_id=user_id)
                return True  # безопасное значение по умолчанию
            
            self.cursor.execute(
                "SELECT enabled FROM user_reminders WHERE user_id = %s",
                (user_id,)
            )
            row = self.cursor.fetchone()

            if not row:
                # создаём запись по умолчанию (только если пользователь зарегистрирован)
                self.init_user_reminder_record(user_id)
                return True

            return row[0]

        except psycopg2.Error as e:
            log_system_event("database", "get_reminders_status_error", error=str(e), user_id=user_id)
            return True  # безопасное значение по умолчанию

    # ---------------------------------------------------------------------
    # Установка статуса
    # ---------------------------------------------------------------------
    def set_reminders_status(self, user_id: int, enabled: bool):
        """
        Устанавливает статус напоминаний для пользователя.
        ВАЖНО: Проверяет существование пользователя в таблице users перед операцией.
        """
        try:
            # Проверка существования пользователя в users
            self.cursor.execute(
                "SELECT 1 FROM users WHERE user_id = %s",
                (user_id,)
            )
            if not self.cursor.fetchone():
                log_system_event("database", "reminders_status_update_skipped", reason="user_not_found", user_id=user_id)
                return  # пользователь не зарегистрирован
            
            self.cursor.execute(
                """
                INSERT INTO user_reminders (user_id, enabled, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (user_id)
                DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = NOW()
                """,
                (user_id, enabled)
            )
            self.conn.commit()
            log_system_event("database", "reminders_status_updated", user_id=user_id, enabled=enabled)

        except psycopg2.Error as e:
            log_system_event("database", "reminders_status_update_error", error=str(e), user_id=user_id)
            self.conn.rollback()

    # ---------------------------------------------------------------------
    # Остальной исходный код
    # ---------------------------------------------------------------------
    # ---------------------------------------------------------------------
    # Утилита для добавления колонок
    # ---------------------------------------------------------------------
    def _add_column_if_not_exists(self, column_name: str, column_type: str, table_name: str = "users"):
        try:
            check_column_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name=%s and column_name=%s;
            """
            self.cursor.execute(check_column_query, (table_name, column_name))
            if not self.cursor.fetchone():
                add_column_query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
                self.cursor.execute(add_column_query)
                self.conn.commit()  # Коммитим сразу
                log_system_event("database", "column_added", column=column_name, table=table_name)
        except psycopg2.Error as e:
            log_system_event("database", "column_add_error", error=str(e), column=column_name, table=table_name)
            if self.conn:
                self.conn.rollback()

    # ----- Оригинальные методы регистрации/валидации (не менялись) -----

    def is_user_registered(self, user_id: int) -> bool:
        try:
            self.cursor.execute("SELECT 1 FROM users WHERE user_id = %s", (user_id,))
            return self.cursor.fetchone() is not None
        except psycopg2.Error as e:
            error_msg = str(e)
            log_system_event("database", "query_failed", error=error_msg, user_id=user_id)
            
            # Если транзакция прервана - делаем rollback и повторяем запрос
            if "текущая транзакция прервана" in error_msg or "current transaction is aborted" in error_msg.lower():
                try:
                    self.conn.rollback()
                    # Повторяем запрос после rollback
                    self.cursor.execute("SELECT 1 FROM users WHERE user_id = %s", (user_id,))
                    result = self.cursor.fetchone() is not None
                    log_system_event("database", "query_retry_success", user_id=user_id)
                    return result
                except Exception as retry_error:
                    log_system_event("database", "query_retry_failed", error=str(retry_error), user_id=user_id)
                    return False
            
            return False

    def get_user_greeting(self, user_id: int) -> str:
        try:
            self.cursor.execute("SELECT fio FROM users WHERE user_id = %s", (user_id,))
            row = self.cursor.fetchone()
            if not row:
                return "гость"
            fio = row[0].split()
            return " ".join(fio[1:]) if len(fio) >= 2 else fio[0]
        except psycopg2.Error:
            return "гость"

    def update_last_chat_id(self, user_id: int, chat_id: int):
        """Обновляет последний известный chat_id пользователя"""
        try:
            self.cursor.execute(
                "UPDATE users SET last_chat_id = %s WHERE user_id = %s",
                (chat_id, user_id)
            )
            self.conn.commit()
        except psycopg2.Error as e:
            log_system_event("database", "update_last_chat_id_failed", error=str(e), user_id=user_id)
            self.conn.rollback()

    def get_last_chat_id(self, user_id: int) -> int:
        """Получает последний известный chat_id пользователя"""
        try:
            self.cursor.execute("SELECT last_chat_id FROM users WHERE user_id = %s", (user_id,))
            row = self.cursor.fetchone()
            return row[0] if row else None
        except psycopg2.Error:
            return None

    @staticmethod
    def normalize_fio(fio: str) -> str:
        """Приводит каждое слово ФИО к формату «Слово» (первая заглавная, остальные строчные)."""
        if not fio or not fio.strip():
            return fio
        fio_cleaned = ' '.join(fio.split())
        words = fio_cleaned.split()
        result = []
        for word in words:
            if '-' in word:
                parts = [p.capitalize() if p else '' for p in word.split('-')]
                result.append('-'.join(parts))
            else:
                result.append(word.capitalize())
        return ' '.join(result)

    def validate_fio(self, fio: str) -> bool:
        fio_normalized = self.normalize_fio(fio)
        return bool(re.match(r"^[А-ЯЁ][а-яё]+(-[А-ЯЁ][а-яё]+)? [А-ЯЁ][а-яё]+ [А-ЯЁ][а-яё]+$", fio_normalized))

    def validate_phone(self, phone: str) -> bool:
        phone_cleaned = re.sub(r'[\s\-]', '', phone)
        return bool(re.match(r"^\+7\d{10}$", phone_cleaned))

    def validate_birth_date(self, date_str: str) -> bool:
        if not re.match(r"^\d{2}\.\d{2}\.\d{4}$", date_str):
            return False

        try:
            day, month, year = map(int, date_str.split('.'))
            birth_date = datetime(year, month, day)
        except ValueError:
            return False

        today = datetime.today()
        if birth_date > today:
            return False

        age = today.year - birth_date.year - (
            (today.month, today.day) < (birth_date.month, birth_date.day)
        )

        return 18 <= age <= 150

    def validate_snils(self, snils: str) -> bool:
        """Простая проверка формата СНИЛС (11 цифр)"""
        snils_cleaned = re.sub(r'[\s\-]', '', snils)
        return bool(re.match(r"^\d{11}$", snils_cleaned))

    def validate_oms(self, oms: str) -> bool:
        """Простая проверка формата ОМС (16 цифр)"""
        oms_cleaned = re.sub(r'[\s\-]', '', oms)
        return bool(re.match(r"^\d{16}$", oms_cleaned))

    def validate_gender(self, gender: str) -> bool:
        return gender in ["Мужской", "Женский"]

    def get_user_phone(self, user_id: int) -> str:
        try:
            self.cursor.execute("SELECT phone FROM users WHERE user_id = %s", (user_id,))
            row = self.cursor.fetchone()
            return row[0] if row else "Не указан"
        except psycopg2.Error:
            return "Не указан"
    
    # Сохраняем обратную совместимость с 'get_user_data' если он использовался (в old code был get_user_data но в приведенном snippet его нет, есть get_user_full_data)
    # на всякий случай, если где-то используется
    def get_user_data(self, user_id: int):
         return self.get_user_full_data(user_id)

    def validate_user_data(self, fio, phone, birth_date, snils=None, oms=None, gender=None):
        base_valid = (
            self.validate_fio(fio)
            and self.validate_phone(phone)
            and self.validate_birth_date(birth_date)
        )
        if snils and not self.validate_snils(snils):
            return False
        if oms and not self.validate_oms(oms):
            return False
        if gender and not self.validate_gender(gender):
            return False
        return base_valid

    def register_user(self, user_id: int, chat_id: int, fio: str, phone: str, birth_date: str, snils: str = None, oms: str = None, gender: str = None) -> bool:
        fio = self.normalize_fio(fio)
        if not self.validate_user_data(fio, phone, birth_date, snils, oms, gender):
            return False

        try:
            reg_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            phone_cleaned = re.sub(r'[\s\-]', '', phone)
            snils_cleaned = re.sub(r'[\s\-]', '', snils) if snils else None
            oms_cleaned = re.sub(r'[\s\-]', '', oms) if oms else None

            self.cursor.execute(
                """
                INSERT INTO users (user_id, last_chat_id, fio, phone, birth_date, snils, oms, gender, registration_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (user_id, chat_id, fio, phone_cleaned, birth_date, snils_cleaned, oms_cleaned, gender, reg_date)
            )
            self.conn.commit()

            # ⚡ Создаём запись о напоминаниях
            self.init_user_reminder_record(user_id)

            return True

        except psycopg2.Error as e:
            log_system_event("database", "user_registration_failed", error=str(e), user_id=user_id)
            self.conn.rollback()
            return False

    def update_user_data(self, user_id: int, fio: str, birth_date: str, snils: str = None, oms: str = None, gender: str = None) -> bool:
        """
        Обновляет данные пользователя в БД.
        Используется для синхронизации с РМИС.
        """
        try:
            # Валидация пропускается или делается частичной, т.к. данные из РМИС считаем "мастер-данными"
            # Но на всякий случай базовую очистку делаем
            fio = self.normalize_fio(fio)
            snils_cleaned = re.sub(r'[\s\-]', '', snils) if snils else None
            oms_cleaned = re.sub(r'[\s\-]', '', oms) if oms else None
            
            self.cursor.execute(
                """
                UPDATE users 
                SET fio = %s, birth_date = %s, snils = %s, oms = %s, gender = %s
                WHERE user_id = %s
                """,
                (fio, birth_date, snils_cleaned, oms_cleaned, gender, user_id)
            )
            self.conn.commit()
            log_system_event("database", "user_data_updated", user_id=user_id)
            return True
        except psycopg2.Error as e:
            log_system_event("database", "user_update_failed", error=str(e), user_id=user_id)
            self.conn.rollback()
            return False

    def add_appointment(self, user_id: int, appointment_data: dict, booking_source: str = 'self_bot') -> bool:
        """
        Сохраняет запись о приеме врача.
        ВАЖНО: Проверяет существование пользователя в таблице users перед вставкой.
        :param user_id: ID пользователя, который создал запись
        :param appointment_data: Полный JSON с данными о записи (включая данные пациента)
        :param booking_source: 'self_bot', 'other_bot', или 'external'
        """
        try:
            import json
            from datetime import datetime
            
            # Проверка существования пользователя в users
            self.cursor.execute(
                "SELECT 1 FROM users WHERE user_id = %s",
                (user_id,)
            )
            if not self.cursor.fetchone():
                log_system_event("database", "appointment_add_skipped", reason="user_not_found", user_id=user_id)
                return False  # пользователь не зарегистрирован
            
            # Извлекаем ключевые поля для удобства (если они есть в JSON)
            # Структура JSON зависит от API, но предполагаем наличие даты и МО
            # Приоритет отдаем start_time, так как там лежит полный timestamp (Date + Time)
            external_mo_name = (
                appointment_data.get('external_mo_name')
                or appointment_data.get('mo_name')
                or appointment_data.get('lpu_name')
                or appointment_data.get('Мед учреждение')
            )

            visit_time_raw = (
                appointment_data.get('external_visit_time')
                or appointment_data.get('start_time')
                or appointment_data.get('visit_time')
                or appointment_data.get('Дата записи')
            )

            def _parse_external_visit_time(value):
                if value is None:
                    return None
                if isinstance(value, datetime):
                    # Если это aware datetime, сохраняем "стеночное" время без tz,
                    # чтобы не зависеть от timezone настроек БД.
                    return value.replace(tzinfo=None) if value.tzinfo else value
                if not isinstance(value, str):
                    value = str(value)
                s = value.strip()
                if not s:
                    return None

                # 1) ISO (включая YYYY-MM-DDTHH:MM:SS+03:00)
                try:
                    dt = datetime.fromisoformat(s)
                    return dt.replace(tzinfo=None) if dt.tzinfo else dt
                except Exception:
                    pass

                # 2) Явные форматы, которые встречаются в проекте
                formats = (
                    "%d.%m.%Y %H:%M:%S",
                    "%d.%m.%Y %H:%M",
                    "%Y-%m-%dT%H:%M:%S%z",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S",
                )
                for fmt in formats:
                    try:
                        dt = datetime.strptime(s, fmt)
                        return dt.replace(tzinfo=None) if dt.tzinfo else dt
                    except ValueError:
                        continue
                return None

            external_visit_time = _parse_external_visit_time(visit_time_raw) or visit_time_raw

            book_id_mis = (
                appointment_data.get("book_id_mis")
                or appointment_data.get("Book_Id_Mis")
            )
            if isinstance(book_id_mis, str):
                book_id_mis = book_id_mis.strip() or None
            
            # Сериализуем JSON
            json_str = json.dumps(appointment_data, ensure_ascii=False)
            
            self.cursor.execute(
                """
                INSERT INTO appointments (
                    user_id, 
                    appointment_json, 
                    book_id_mis,
                    external_visit_time, 
                    external_mo_name, 
                    created_at, 
                    status, 
                    booking_source
                )
                VALUES (%s, %s, %s, %s, %s, NOW(), 'active', %s)
                ON CONFLICT (user_id, book_id_mis)
                DO UPDATE SET
                    appointment_json = COALESCE(appointments.appointment_json, '{}'::jsonb) || EXCLUDED.appointment_json,
                    external_visit_time = COALESCE(EXCLUDED.external_visit_time, appointments.external_visit_time),
                    external_mo_name = COALESCE(EXCLUDED.external_mo_name, appointments.external_mo_name),
                    status = 'active',
                    cancelled_at = NULL
                """,
                (user_id, json_str, book_id_mis, external_visit_time, external_mo_name, booking_source)
            )
            self.conn.commit()
            log_system_event("database", "appointment_added", user_id=user_id)
            return True
        except psycopg2.IntegrityError as e:
            # Если остался уникальный индекс idx_appointments_user_visit_mo (старое поведение),
            # запись может конфликтовать по (user_id, external_visit_time, external_mo_name).
            # В этом случае не падаем — обновляем существующую строку и проставляем book_id_mis.
            constraint = getattr(getattr(e, "diag", None), "constraint_name", None)
            pgcode = getattr(e, "pgcode", None)
            if pgcode == "23505" and constraint == "idx_appointments_user_visit_mo":
                try:
                    if self.conn:
                        self.conn.rollback()

                    # Находим существующую запись по ключу (user_id, visit_time, mo_name)
                    self.cursor.execute(
                        """
                        SELECT id, book_id_mis
                        FROM appointments
                        WHERE user_id = %s
                          AND external_visit_time = %s
                          AND external_mo_name = %s
                        LIMIT 1
                        """,
                        (user_id, external_visit_time, external_mo_name),
                    )
                    row = self.cursor.fetchone()
                    if not row:
                        log_system_event("database", "appointment_add_failed", error=str(e), user_id=user_id)
                        return False

                    existing_id = row[0]
                    existing_book_id = row[1]

                    # Мерджим JSON в существующую строку и обновляем book_id_mis
                    try:
                        import json as _json
                        patch = dict(appointment_data)
                        if existing_book_id and book_id_mis and str(existing_book_id) != str(book_id_mis):
                            patch.setdefault("Book_Id_Mis_Original", str(existing_book_id))
                        patch_json = _json.dumps(patch, ensure_ascii=False)
                    except Exception:
                        patch_json = json_str

                    self.cursor.execute(
                        """
                        UPDATE appointments
                        SET appointment_json = COALESCE(appointment_json, '{}'::jsonb) || %s::jsonb,
                            book_id_mis = COALESCE(%s, book_id_mis),
                            status = 'active',
                            cancelled_at = NULL
                        WHERE id = %s
                        """,
                        (patch_json, book_id_mis, existing_id),
                    )
                    self.conn.commit()
                    log_system_event("database", "appointment_added", user_id=user_id)
                    return True
                except Exception as inner:
                    log_system_event("database", "appointment_add_failed", error=str(inner), user_id=user_id)
                    if self.conn:
                        self.conn.rollback()
                    return False

            log_system_event("database", "appointment_add_failed", error=str(e), user_id=user_id)
            if self.conn:
                self.conn.rollback()
            return False
        except psycopg2.Error as e:
            log_system_event("database", "appointment_add_failed", error=str(e), user_id=user_id)
            self.conn.rollback()
            return False

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


# Экземпляр базы данных
db = UserDatabase()
