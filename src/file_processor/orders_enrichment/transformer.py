import logging
from typing import Dict, Any, List
from file_processor.user_filter.transformations import batch_transform

logger = logging.getLogger(__name__)

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