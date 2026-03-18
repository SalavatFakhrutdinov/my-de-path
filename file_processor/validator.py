import logging
from typing import List, Dict, Any, Tuple, Optional
from constants import *

logger = logging.getLogger(__name__)


"""
Проверка одного пользователя
"""


def validate_user(user: Dict[str, Any], line_num: Optional[int] = None) -> Tuple[bool, List[str]]:
    errors = []
    location = f"в строке {line_num}" if line_num else ""

    if not isinstance(user, dict):
        errors.append(
            f"Пользователь {location} не входит в словарь: {type(user).__name__}"
        )
        return False, errors

    missing = REQUIRED_FIELDS - user.keys()
    if missing:
        errors.append(f"У пользователя {location} пропущены поля: {missing}")
        return False, errors

    try:
        user_id = user["id"]
        if not isinstance(usser_id, (int, float)) or isinstance(user_id, bool):
            errors.append(f"Пользователь {location}: поле id должно быть числовым, получен {type(user_id).__name__} тип")
        
        name = user["name"]
        if not isinstance(name, str):
            errors.append(
                f"Пользователь {location}: поле name должно быть строкового типа, получен тип {type(name).__name__}"
            )
        elif not name.strip():
            errors.append(f"Пользователь {location}: поле name не может быть пустым")

        age = user["age"]
        if not isinstance(age, (int, float)) or isinstance(age, bool):
            errors.append(
                f"Пользователь {location}: поле age должно быть числовым, получен тип {type(age).__name__}"
            )
        elif age < MIN_AGE or age > MAX_AGE:
            errors.append(
                f"Пользователь {location}: поле age должно быть в диапазоне между {MIN_AGE} и {MAX_AGE}, получено значение {age}"
            )

    except Exception as e:
        errors.append(f"Пользователь {location}: ошибка валидации: {e}")

    return len(errors) == 0, errors


"""
Фильтр совершеннолетних пользователей
"""


def filter_adults(
    users: List[Dict[str, Any]], min_age: int = 18
) -> List[Dict[str, Any]]:

    logger.debug(f"Фильтрация пользователей по возрасту >= {min_age}")

    adults = [user for user in users if user["age"] >= min_age]
    logger.info(f"Обнаружено {len(adults)} совершеннолетних пользователей")
    return adults


"""
Сортировка пользователей по возрасту
"""


def sort_by_age(
    users: List[Dict[str, Any]], reverse: bool = False
) -> List[Dict[str, Any]]:

    order = "убывания" if reverse else "возрастания"
    logger.debug(f"Сортировка {len(users)} пользователей по возрасту в порядке {order}")

    sorted_users = sorted(users, key=lambda x: x["age"], reverse=reverse)

    if logger.isEnabledFor(logging.DEBUG):
        ages = [user["age"] for user in sorted_users[:5]]
        logger.debug(f"Возраста после сортировки: {ages}")

    return sorted_users
