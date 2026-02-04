# sync_appointments/parser.py
"""
Парсинг JSON данных из внешней системы МИС.
"""

import json
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta, date

from .utils import (
    normalize_phone,
    normalize_birth_date,
    normalize_fio,
    parse_datetime,
    extract_doctor_info
)

logger = logging.getLogger(__name__)


class Parser:
    """
    Класс для парсинга данных из внешней системы МИС.
    """

    def __init__(self):
        self.processed_count = 0
        self.error_count = 0

    def parse_response(self, json_data: str) -> List[Dict[str, Any]]:
        """
        Парсит JSON ответ от внешней системы.

        Args:
            json_data: JSON строка или словарь с данными

        Returns:
            Список нормализованных записей
        """
        try:
            # Если пришла строка, парсим её
            if isinstance(json_data, str):
                data = json.loads(json_data)
            else:
                data = json_data

            # Получаем массив записей
            informer_result = data.get('InformerResult', [])

            if not isinstance(informer_result, list):
                logger.error(f"InformerResult не является массивом: {type(informer_result)}")
                return []

            logger.info(f"Получено {len(informer_result)} записей для обработки")

            # Обрабатываем каждую запись
            parsed_records = []
            for i, record in enumerate(informer_result):
                try:
                    parsed_record = self._parse_single_record(record)
                    if parsed_record:
                        parsed_records.append(parsed_record)
                        self.processed_count += 1
                    else:
                        self.error_count += 1
                except Exception as e:
                    self.error_count += 1
                    logger.error(f"Ошибка обработки записи {i}: {e}")
                    continue

            logger.info(f"Успешно обработано {self.processed_count} записей, ошибок: {self.error_count}")
            return parsed_records

        except json.JSONDecodeError as e:
            logger.error(f"Ошибка декодирования JSON: {e}")
            return []
        except Exception as e:
            logger.error(f"Ошибка парсинга ответа: {e}")
            return []

    def _parse_single_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Парсит одну запись из массива InformerResult.

        Args:
            record: Словарь с данными одной записи

        Returns:
            Нормализованный словарь с данными или None если ошибка
        """
        try:
            # Извлекаем основные поля
            last_name = (record.get('Last_Name') or '').strip()
            first_name = (record.get('First_Name') or '').strip()
            middle_name = (record.get('Middle_Name') or '').strip()
            birth_date = record.get('Birth_Date') or ''
            mobile_phone = record.get('Mobile_Phone') or ''
            mo_name = (record.get('MO_Name') or '').strip()
            mo_address = (record.get('MO_Adress') or '').strip()
            specialist_name = (record.get('Specialist_Name') or '').strip()
            visit_time_str = record.get('VisitTime') or ''
            
            # Извлекаем дополнительные поля для сохранения в appointment_json
            book_id_mis = record.get('Book_Id_Mis', '')
            patient_id = record.get('PatientID', '')
            
            # Преобразуем в строковый формат (text)
            book_id_mis_str = str(book_id_mis) if book_id_mis is not None else ''
            patient_id_str = str(patient_id) if patient_id is not None else ''

            # Проверяем обязательные поля
            if not all([last_name, first_name, birth_date, mobile_phone, visit_time_str]):
                logger.warning(f"Пропущена запись с отсутствующими обязательными полями")
                return None

            # Нормализуем данные для сопоставления
            normalized_fio = normalize_fio(last_name, first_name, middle_name)
            normalized_phones = normalize_phone(mobile_phone)
            normalized_birth_date = normalize_birth_date(birth_date)

            if not normalized_phones:
                logger.warning(f"Нет валидных телефонов для {normalized_fio}")
                return None

            if not normalized_birth_date:
                logger.warning(f"Некорректная дата рождения для {normalized_fio}")
                return None

            # Парсим время приема
            visit_time = parse_datetime(visit_time_str)
            if not visit_time:
                logger.warning(f"Некорректное время приема для {normalized_fio}: {visit_time_str}")
                return None

            # Проверяем, что запись именно на завтра
            today = date.today()
            tomorrow = today + timedelta(days=1)
            visit_date = visit_time.date()
            
            if visit_date != tomorrow:
                logger.debug(f"Запись не на завтра для {normalized_fio}: дата записи {visit_date}, завтра {tomorrow}. Пропускаем.")
                return None

            # Извлекаем информацию о враче
            doctor_fio, doctor_position = extract_doctor_info(specialist_name)

            # Формируем полное ФИО для пользователя
            full_fio = f"{last_name} {first_name} {middle_name}".strip()

            # Формируем результат
            parsed_record = {
                # Данные для сопоставления
                'matching_data': {
                    'fio': normalized_fio,
                    'phones': normalized_phones,
                    'birth_date': normalized_birth_date,
                    'full_fio': full_fio
                },
                # Данные для сохранения
                'appointment_data': {
                    'Дата записи': visit_time_str,
                    'Мед учреждение': mo_name,
                    'Адрес мед учреждения': mo_address,
                    'ФИО врача': doctor_fio,
                    'Должность врача': doctor_position,
                    'Book_Id_Mis': book_id_mis_str,
                    'PatientID': patient_id_str,
                    'Исходные_данные': {  # Для отладки
                        'ФИО пациента': full_fio,
                        'Дата рождения': birth_date,
                        'Телефон': mobile_phone
                    }
                },
                # Метаданные
                'metadata': {
                    'visit_time': visit_time,
                    'mo_name': mo_name,
                    'original_data': record  # Сохраняем оригинальные данные
                }
            }

            return parsed_record

        except Exception as e:
            logger.error(f"Ошибка парсинга отдельной записи: {e}")
            return None

    def get_stats(self) -> Dict[str, Any]:
        """
        Возвращает статистику парсинга.

        Returns:
            Словарь со статистикой
        """
        return {
            'processed': self.processed_count,
            'errors': self.error_count,
            'success_rate': (self.processed_count / (self.processed_count + self.error_count) * 100)
            if (self.processed_count + self.error_count) > 0 else 0
        }

    def create_user_friendly_json(self, appointment_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Создает JSON для сохранения в удобном для пользователя формате.

        Args:
            appointment_data: Данные о записи

        Returns:
            JSON для сохранения в БД
        """
        try:
            # Убираем отладочные данные
            if 'Исходные_данные' in appointment_data:
                appointment_data_copy = appointment_data.copy()
                del appointment_data_copy['Исходные_данные']
                return appointment_data_copy

            return appointment_data

        except Exception as e:
            logger.error(f"Ошибка создания пользовательского JSON: {e}")
            return appointment_data

    def validate_record_completeness(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Проверяет полноту данных в записи.

        Args:
            record: Запись для проверки

        Returns:
            (is_valid, missing_fields)
        """
        missing_fields = []

        required_fields = ['Last_Name', 'First_Name', 'Birth_Date', 'Mobile_Phone', 'VisitTime']

        for field in required_fields:
            value = record.get(field, '').strip()
            if not value:
                missing_fields.append(field)

        return len(missing_fields) == 0, missing_fields

    def batch_parse(self, json_data_list: List[str]) -> List[Dict[str, Any]]:
        """
        Парсит несколько JSON ответов.

        Args:
            json_data_list: Список JSON строк

        Returns:
            Объединенный список записей
        """
        all_records = []

        for i, json_data in enumerate(json_data_list):
            try:
                records = self.parse_response(json_data)
                all_records.extend(records)
                logger.info(f"Батч {i + 1}: обработано {len(records)} записей")
            except Exception as e:
                logger.error(f"Ошибка обработки батча {i + 1}: {e}")
                continue

        return all_records