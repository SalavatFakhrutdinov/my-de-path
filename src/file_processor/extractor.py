import json
import logging
from typing import List, Dict, Any, Optional, Iterator

from file_processor.reader import retry, RETRYABLE_EXCEPTIONS

logger = logging.getLogger(__name__)


@retry(max_attempts=3, delay=1.0)
def _read_json_file(filepath: str) -> List[Dict[str, Any]]:
    with open(filepath, "r", encoding="utf-8") as file:
        data = json.load(file)

    if not isinstance(data, list):
        raise ValueError(f"Ожидался JSON массив, получен {type(data).__name__}")
    
    return data


def extract_users(filepath: str) -> Optional[List[Dict[str, Any]]]:
    logger.info(f"Извлечение пользователей из {filepath}")

    try:
        users = _read_json_file(filepath)
        logger.info(f"Извлечено {len(users)} пользователей")
        return users
    except Exception as e:
        logger.error(f"Ошибка извлечения пользователей: {e}")
        return None
    

def extract_orders(filepath: str) -> Optional[List[Dict[str, Any]]]:
    logger.info(f"Извлечение заказов из {filepath}")

    try:
        orders = _read_json_file(filepath)
        logger.info(f"Извлечено {len(orders)} заказов")
        return orders
    except Exception as e:
        logger.error(f"Ошибка извлечения заказов: {e}")
        return None