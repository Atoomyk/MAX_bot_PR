# sync_appointments/matcher.py
"""
Сопоставление пациентов из внешней системы с пользователями бота.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class Matcher:
    """
    Класс для сопоставления пациентов из МИС с пользователями бота.
    """

    def __init__(self, user_database):
        """
        Инициализация с подключением к базе пользователей.

        Args:
            user_database: Объект базы данных пользователей
        """
        self.db = user_database
        self.matched_count = 0
        self.unmatched_count = 0

    def find_user_by_patient_data(self, patient_data: Dict[str, Any]) -> Optional[str]:
        """
        Находит пользователя бота по данным пациента из МИС.

        Args:
            patient_data: Данные пациента из парсера

        Returns:
            chat_id пользователя или None если не найден
        """
        try:
            matching_data = patient_data.get('matching_data', {})
            normalized_phones = matching_data.get('phones', [])
            if not normalized_phones:
                logger.warning("Невозможно выполнить сопоставление: отсутствует телефон в данных пациента")
                self.unmatched_count += 1
                return None

            chat_id = self._find_user_by_phone(normalized_phones)
            if chat_id is not None:
                self.matched_count += 1
                logger.info(f"Найден пользователь по телефону: chat_id={chat_id}")
                return chat_id

            logger.debug(f"Пользователь не найден по телефонам: {normalized_phones}")
            self.unmatched_count += 1
            return None

        except Exception as e:
            logger.error(f"Ошибка поиска пользователя: {e}")
            self.unmatched_count += 1
            return None

    def _find_user_by_phone(self, normalized_phones: List[str]) -> Optional[str]:
        """
        Ищет пользователя по одному из нормализованных телефонных номеров.

        Args:
            normalized_phones: Список нормализованных номеров из МИС

        Returns:
            chat_id пользователя или None если не найден
        """
        try:
            for phone in normalized_phones:
                if not phone:
                    continue
                query = "SELECT user_id FROM users WHERE phone = %s"
                self.db.cursor.execute(query, (phone,))
                rows = self.db.cursor.fetchall()

                if not rows:
                    continue

                if len(rows) > 1:
                    logger.warning(f"Найдено несколько пользователей с одинаковым телефоном {phone}, берем первого")

                return rows[0][0]

            return None
        except Exception as e:
            logger.error(f"Ошибка поиска пользователя по телефону: {e}")
            return None

    def _find_users_by_birth_date(self, birth_date: str) -> List[Tuple]:
        """
        Находит пользователей по дате рождения.

        Args:
            birth_date: Дата рождения в формате YYYY-MM-DD

        Returns:
            Список кортежей с данными пользователей
        """
        try:
            # Преобразуем формат даты из YYYY-MM-DD в DD.MM.YYYY для поиска в БД
            try:
                date_obj = datetime.strptime(birth_date, '%Y-%m-%d')
                db_date_format = date_obj.strftime('%d.%m.%Y')
            except ValueError:
                # Если не удалось преобразовать, пробуем искать как есть
                db_date_format = birth_date

            query = "SELECT user_id, fio, phone, birth_date FROM users WHERE birth_date = %s"
            self.db.cursor.execute(query, (db_date_format,))
            users = self.db.cursor.fetchall()

            logger.debug(f"Найдено {len(users)} пользователей с датой рождения {db_date_format}")
            return users

        except Exception as e:
            logger.error(f"Ошибка поиска пользователей по дате рождения: {e}")
            return []

    def _match_fio(self, chat_id: str, normalized_fio: str) -> bool:
        """
        Проверяет совпадение ФИО.
        Сравнивает полное ФИО без учета порядка слов.
        """
        try:
            query = "SELECT fio FROM users WHERE user_id = %s"
            self.db.cursor.execute(query, (chat_id,))
            result = self.db.cursor.fetchone()

            if not result:
                return False

            db_fio = result[0].strip().upper() if result[0] else ""

            if not db_fio:
                return False

            # Разбиваем ФИО на части и сортируем
            db_parts = sorted(db_fio.split())
            mis_parts = sorted(normalized_fio.split())

            # Сравниваем наборы слов
            return db_parts == mis_parts

        except Exception as e:
            logger.error(f"Ошибка сравнения ФИО для chat_id={chat_id}: {e}")
            return False

    def _match_phone(self, chat_id: str, normalized_phones: List[str]) -> bool:
        """
        Проверяет совпадение телефона (хотя бы один номер).

        Args:
            chat_id: ID пользователя
            normalized_phones: Список нормализованных номеров из МИС

        Returns:
            True если хотя бы один номер совпадает
        """
        try:
            query = "SELECT phone FROM users WHERE user_id = %s"
            self.db.cursor.execute(query, (chat_id,))
            result = self.db.cursor.fetchone()

            if not result:
                return False

            db_phone = result[0].strip() if result[0] else ""

            if not db_phone:
                return False

            # Проверяем совпадение с каждым номером из МИС
            for mis_phone in normalized_phones:
                if db_phone == mis_phone:
                    logger.debug(f"Совпадение телефона: БД={db_phone}, МИС={mis_phone}")
                    return True

            # Если прямого совпадения нет, попробуем сравнить без +
            db_phone_clean = db_phone.lstrip('+')
            for mis_phone in normalized_phones:
                mis_phone_clean = mis_phone.lstrip('+')
                if db_phone_clean == mis_phone_clean:
                    logger.debug(f"Совпадение телефона (без +): БД={db_phone_clean}, МИС={mis_phone_clean}")
                    return True

            logger.debug(f"Не найдено совпадения телефонов: БД={db_phone}, МИС={normalized_phones}")
            return False

        except Exception as e:
            logger.error(f"Ошибка сравнения телефона для chat_id={chat_id}: {e}")
            return False

    def batch_match(self, patients_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Пакетное сопоставление пациентов с пользователями.

        Args:
            patients_data: Список данных пациентов

        Returns:
            Словарь с результатами:
            - 'matched': список найденных записей с user_id
            - 'unmatched': список ненайденных записей
        """
        results = {
            'matched': [],
            'unmatched': []
        }

        for patient_data in patients_data:
            try:
                user_id = self.find_user_by_patient_data(patient_data)

                if user_id:
                    results['matched'].append({
                        'user_id': user_id,
                        'patient_data': patient_data
                    })
                else:
                    results['unmatched'].append(patient_data)

            except Exception as e:
                logger.error(f"Ошибка при сопоставлении пациента: {e}")
                results['unmatched'].append(patient_data)

        logger.info(
            f"Сопоставление завершено: найдено {len(results['matched'])}, не найдено {len(results['unmatched'])}")
        return results

    def get_user_reminders_status(self, chat_id: str) -> bool:
        """
        Проверяет, включены ли уведомления у пользователя.

        Args:
            chat_id: ID пользователя

        Returns:
            True если уведомления включены
        """
        try:
            # Используем метод из user_database
            return self.db.get_reminders_status(chat_id)
        except Exception as e:
            logger.error(f"Ошибка проверки статуса уведомлений для {chat_id}: {e}")
            return False  # По умолчанию не отправляем, если ошибка

    def get_stats(self) -> Dict[str, Any]:
        """
        Возвращает статистику сопоставления.

        Returns:
            Словарь со статистикой
        """
        return {
            'matched': self.matched_count,
            'unmatched': self.unmatched_count,
            'total_processed': self.matched_count + self.unmatched_count,
            'match_rate': (self.matched_count / (self.matched_count + self.unmatched_count) * 100)
            if (self.matched_count + self.unmatched_count) > 0 else 0
        }

    def validate_user_exists(self, chat_id: str) -> bool:
        """
        Проверяет, существует ли пользователь в БД.

        Args:
            chat_id: ID пользователя

        Returns:
            True если пользователь существует
        """
        try:
            query = "SELECT 1 FROM users WHERE user_id = %s"
            self.db.cursor.execute(query, (chat_id,))
            return self.db.cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Ошибка проверки существования пользователя {chat_id}: {e}")
            return False