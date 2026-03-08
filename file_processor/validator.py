import logging
from typing import List, Dict, Any, Tuple
from constants import *

logger = logging.getLogger(__name__)


"""
Проверка одного пользователя
"""
def validate_user(user: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errors = []
    missing = REQUIRED_FIELDS - user.keys()
    if missing:
        errors.append(f"Пропущены поля: {missing}")
        return False, errors
    
    user_id = user['id']
    if not isinstance(user_id, (int, float)) or isinstance(user_id, bool):
        errors.append(f"Поле id должен быть числового типа,"
                        "получен тип {type(name).__name__}")
    
    name = user['name']
    if not isinstance(name, str):
        errors.append(f"Поле name должно быть строкового типа,"
                        "получен тип {type(name).__name__}")
    elif not name.strip():
        errors.append("Поле name не может быть пустым")

    age = user['age']
    if not isinstance(age, (int, float)) or isinstance(age, bool):
        errors.append(f"Поле age должно быть числовым, получен"
                        "тип {type(age).__name__}")
    elif age < MIN_AGE or age > MAX_AGE:
        errors.append(f"Поле age должно быть в диапазоне между"
                        "{MIN_AGE} и {MAX_AGE}, получено значение {age}")
    
    return len(errors) == 0, errors


"""
Проверка списка пользователей
"""
def validate_users(
        users: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    
    logger.info(f"Проверка {len(users)} пользователей")

    valid_users = []
    invalid_users = []

    for idx, user in enumerate(users):
        is_valid, errors = validate_user(user)

        if is_valid:
            valid_users.append(user)
        else:
            invalid_entry = {
                'index': idx,
                'user': user,
                'errors': errors
            }
            invalid_users.append(invalid_entry)
            logger.warning(f"Невалидный пользователь"
                           "по индексу {idx}: {errors}")
    
    logger.info(f"Валидация заверщена: {len(valid_users)} валидных,"
                f"невалидных: {len(invalid_users)}")
    
    return valid_users, invalid_users


"""
Фильтр совершеннолетних пользователей
"""
def filter_adults(
        users: List[Dict[str, Any]],
        min_age: int = 18
) -> List[Dict[str, Any]]:
    
    logger.debug(f"Фильтрация пользователей по возрасту >= {min_age}")

    adults = [user for user in users if user['age'] >= min_age]
    logger.info(f"Обнаружено {len(adults)} совершеннолетних пользователей")
    return adults


"""
Сортировка пользователей по возрасту
"""
def sort_by_age(
        users: List[Dict[str, Any]],
        reverse: bool = False
) -> List[Dict[str, Any]]:
    
    order = "убывания" if reverse else "возрастания"
    logger.debug(f"Сортировка {len(users)} пользователей по возрасту"
                 f"в порядке {order}")
    
    sorted_users = sorted(users, key=lambda x: x['age'], reverse=reverse)

    if logger.isEnabledFor(logging.DEBUG):
        ages = [user['age'] for user in sorted_users]
        logger.debug(f"Возраста после сортировки: {ages}")

    return sorted_users