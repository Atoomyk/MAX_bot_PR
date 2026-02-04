"""
Модуль интеграции с ЕСИА (Единая система идентификации и аутентификации)
Обработка авторизации пользователей через ЕСИА и сохранение данных
"""

__version__ = "1.0.0"

from .service import (
    generate_esia_url,
    wait_for_esia_file,
    parse_esia_file,
    save_esia_data_to_db,
    delete_esia_file
)

__all__ = [
    'generate_esia_url',
    'wait_for_esia_file',
    'parse_esia_file',
    'save_esia_data_to_db',
    'delete_esia_file'
]
