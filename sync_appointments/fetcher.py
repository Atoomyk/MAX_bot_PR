# sync_appointments/fetcher.py
"""
Получение данных из внешней системы МИС.
"""

import aiohttp
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import os

logger = logging.getLogger(__name__)


class Fetcher:
    """
    Класс для получения данных из внешней системы МИС.
    """

    def __init__(self, base_url: str = None, max_retries: int = 10, retry_delay: int = 600):
        """
        Инициализация fetcher.

        Args:
            base_url: Базовый URL API (если None, берется из переменной окружения)
            max_retries: Максимальное количество попыток при ошибке
            retry_delay: Задержка между попытками в секундах
        """
        self.base_url = base_url or os.getenv('MIS_API_URL', '')
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = None

        if not self.base_url:
            logger.warning("MIS_API_URL не установлен в переменных окружения")

    async def __aenter__(self):
        """Контекстный менеджер для создания сессии."""
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Контекстный менеджер для закрытия сессии."""
        if self.session:
            await self.session.close()

    def _get_date_range(self) -> tuple:
        """
        Генерирует диапазон дат для запроса.

        Returns:
            Кортеж (date1, date2) в формате YYYY-MM-DDTHH:MM:SS
        """
        # Завтрашний день
        tomorrow = datetime.now() + timedelta(days=1)
        date1 = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)

        # Следующий день после завтра
        date2 = date1 + timedelta(days=1)

        # Форматируем для API
        date1_str = date1.strftime('%Y-%m-%dT%H:%M:%S')
        date2_str = date2.strftime('%Y-%m-%dT%H:%M:%S')

        logger.info(f"Диапазон дат для запроса: с {date1_str} по {date2_str}")
        return date1_str, date2_str

    def _build_url(self) -> str:
        """
        Строит полный URL для запроса.

        Returns:
            Полный URL с параметрами
        """
        date1, date2 = self._get_date_range()

        # Базовый URL уже может содержать параметры, добавляем безопасно
        if '?' in self.base_url:
            url = f"{self.base_url}&Date1={date1}&Date2={date2}&Status=1"
        else:
            url = f"{self.base_url}?Date1={date1}&Date2={date2}&Status=1"

        logger.debug(f"Сформирован URL: {url}")
        return url

    async def fetch_data(self, use_retry: bool = True) -> Optional[Dict[str, Any]]:
        """
        Получает данные из внешней системы.

        Args:
            use_retry: Использовать ли повторные попытки при ошибке

        Returns:
            JSON данные или None при ошибке
        """
        if not self.base_url:
            logger.error("Не указан URL для запроса данных")
            return None

        if not self.session:
            self.session = aiohttp.ClientSession()

        url = self._build_url()

        if use_retry:
            return await self._fetch_with_retry(url)
        else:
            return await self._fetch_single(url)

    async def _fetch_single(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Выполняет один запрос к API.

        Args:
            url: Полный URL для запроса

        Returns:
            JSON данные или None при ошибке
        """
        try:
            logger.info(f"Отправка запроса к {url}")

            timeout = aiohttp.ClientTimeout(total=30)  # 30 секунд таймаут

            async with self.session.get(url, timeout=timeout) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Успешный ответ от сервера, получено данных")
                    return data
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка HTTP {response.status}: {error_text[:200]}")
                    return None

        except aiohttp.ClientError as e:
            logger.error(f"Ошибка сети при запросе к {url}: {e}")
            return None
        except asyncio.TimeoutError:
            logger.error(f"Таймаут при запросе к {url}")
            return None
        except Exception as e:
            logger.error(f"Неожиданная ошибка при запросе: {e}")
            return None

    async def _fetch_with_retry(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Выполняет запрос с повторными попытками.

        Args:
            url: Полный URL для запроса

        Returns:
            JSON данные или None при ошибке после всех попыток
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"Попытка {attempt}/{self.max_retries}")

                data = await self._fetch_single(url)

                if data is not None:
                    logger.info(f"Успешно получены данные с попытки {attempt}")
                    return data

                # Если данные не получены и это не последняя попытка
                if attempt < self.max_retries:
                    logger.warning(f"Попытка {attempt} не удалась, повтор через {self.retry_delay} секунд")
                    await asyncio.sleep(self.retry_delay)
                else:
                    logger.error(f"Все {self.max_retries} попыток не удались")

            except Exception as e:
                logger.error(f"Ошибка на попытке {attempt}: {e}")
                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_delay)

        return None

    async def fetch_from_file(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Получает данные из файла (для тестирования).

        Args:
            file_path: Путь к файлу с тестовыми данными

        Returns:
            JSON данные или None при ошибке
        """
        file_path = "commands/mock_response.json"
        try:
            import json

            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            logger.info(f"Загружены тестовые данные из файла {file_path}")
            return data

        except FileNotFoundError:
            logger.error(f"Файл не найден: {file_path}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка чтения JSON из файла {file_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Ошибка чтения файла {file_path}: {e}")
            return None

    async def health_check(self) -> bool:
        """
        Проверяет доступность внешней системы.

        Returns:
            True если система доступна
        """
        if not self.base_url:
            return False

        try:
            # Берем базовый URL без параметров для проверки
            base_url_clean = self.base_url.split('?')[0]

            timeout = aiohttp.ClientTimeout(total=10)

            async with aiohttp.ClientSession() as session:
                async with session.get(base_url_clean, timeout=timeout) as response:
                    # Проверяем, что сервер отвечает (любой статус кроме 5xx)
                    return response.status < 500

        except Exception as e:
            logger.debug(f"Health check failed: {e}")
            return False

    def get_request_info(self) -> Dict[str, Any]:
        """
        Возвращает информацию о текущем запросе.

        Returns:
            Словарь с информацией
        """
        date1, date2 = self._get_date_range()
        url = self._build_url()

        return {
            'base_url': self.base_url,
            'date_range': {
                'from': date1,
                'to': date2
            },
            'full_url': url,
            'retry_config': {
                'max_retries': self.max_retries,
                'retry_delay_seconds': self.retry_delay
            }
        }