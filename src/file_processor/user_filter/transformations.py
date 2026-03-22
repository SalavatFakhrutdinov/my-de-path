import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


"""
Приведение поля name к нижнему регистру
"""


def normalize_name(user: Dict[str, Any]) -> Dict[str, Any]:
    transformed_name = user.copy()
    original_name = user.get("name", "")

    transformed_name["name"] = original_name.lower().strip()

    if original_name != transformed_name["name"]:
        logger.debug(
            f"Приведено имя: '{original_name}' -> '{transformed_name['name']}'"
        )

    return transformed_name


"""
Добавление возрастной группы на основе возраста
"""


def add_age_group(user: Dict[str, Any]) -> Dict[str, Any]:
    transformed_age = user.copy()
    age = user.get("age", 0)

    if age < 18:
        transformed_age["age_group"] = "дети"
    elif age < 30:
        transformed_age["age_group"] = "молодые люди"
    elif age < 50:
        transformed_age["age_group"] = "взрослые"
    else:
        transformed_age["age_group"] = "старые"

    logger.debug(
        f"Добавлена возрастная группа '{transformed_age['age_group']} для возраста {age} лет"
    )

    return transformed_age


"""
Применение всех доступных трансформаций к пользователю
"""


def apply_transformations(user: Dict[str, Any]) -> Dict[str, Any]:
    transformed_name = normalize_name(user)
    transformed = add_age_group(transformed_name)

    return transformed


"""
Применение трансформаций к списку пользователей
"""


def batch_transform(users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    logger.info(f"Запуск трансформаций для {len(users)} пользователей")
    transformed = [apply_transformations(user) for user in users]
    logger.debug("Трансформация завершена")
    return transformed
