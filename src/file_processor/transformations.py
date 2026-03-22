import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

# Трансформации пользователей
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

# Трансформации для ETL-pipeline (JOIN)
"""
Построение словаря для быстрого доступа к пользователям по ID
"""
def build_user_map(users: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    user_map = {}
    for user in users:
        user_id = users.get("id")
        if user_id is not None:
            user_map[user_id] = user
        else:
            logger.warning(f"Пользователь без id: {user}")

    logger.debug(f"Построен словарь с {len(user_map)} пользователями")
    return user_map


"""
LEFT JOIN пользователей с заказами
"""
def join_users_orders(
    users: List[Dict[str, Any]],
    orders: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    logger.info(f"JOIN: {len(users)} пользователей, {len(orders)} заказов")

    user_map = build_user_map(users)

    enriched = []
    skipped = 0

    for order in orders:
        user_id = order.get("user_id")
        user = user_map.get(user_id)

        if user is None:
            logger.warning(
                f"Заказ {order.get('order_id')}: пользователь {user_id} не найден"
            )
            skipped += 1
            continue

        enriched_order = {
            "user_id": user_id,
            "name": user.get("name"),
            "age": user.get("age"),
            "order_id": order.get("order_id"),
            "amount": order.get("amount"),
            "created_at": order.get("created_at"),
        }
        enriched.append(enriched_order)

    logger.info(f"Обогащено {len(enriched)} заказов "
                f"пропущено {skipped}")
    return enriched


"""
Обогащение заказов данными пользователей с опциональным применением трансформаций
"""
def enrich_orders_with_user_data(
    users: List[Dict[str, Any]],
    orders: List[Dict[str, Any]],
    apply_user_transformations: bool = False
) -> List[Dict[str, Any]]:
    processed_users = users
    if apply_user_transformations:
        processed_users = batch_transform(users)
        logger.info(f"К пользователям применены трансформации")

    return join_users_orders(processed_users, orders)