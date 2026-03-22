import json
import logging
from typing import List, Dict, Any, Optional, Iterator

from file_processor.common.reader import read_json_streaming, read_json_as_list

logger = logging.getLogger(__name__)


"""
Извлечение всех пользователей/заказов из файла
"""
def extract_users(filepath: str) -> Optional[List[Dict[str, Any]]]:
    logger.info(f"Извлечение пользователей из {filepath}")

    try:
        users = read_json_as_list(filepath)
        logger.info(f"Извлечено {len(users)} пользователей")
        return users
    except Exception as e:
        logger.error(f"Ошибка извлечения пользователей: {e}")
        return None
    

def extract_orders(filepath: str) -> Optional[List[Dict[str, Any]]]:
    logger.info(f"Извлечение заказов из {filepath}")

    try:
        orders = read_json_as_list(filepath)
        logger.info(f"Извлечено {len(orders)} заказов")
        return orders
    except Exception as e:
        logger.error(f"Ошибка извлечения заказов: {e}")
        return None
    

"""
Извлечение пользователей/заказов потоком
"""
def extract_users_streaming(filepath: str) -> Iterator[Dict[str, Any]]:
    logger.info(f"Потоковое извлечение пользователей из {filepath}")
    yield from read_json_streaming(filepath)

def extract_orders_streaming(filepath: str) -> Iterator[Dict[str, Any]]:
    logger.info(f"Потоковое извлечение заказов из {filepath}")
    yield from read_json_streaming(filepath)


"""
Извлечение множество ID пользователей
"""
def get_user_ids(users: List[Dict[str, Any]]) -> set:
    return {user.get("id") for user in users if user.get("id") is not None}