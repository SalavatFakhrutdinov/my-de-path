import logging
from typing import List, Dict, Any, Tuple, Optional
from file_processor.common.constants import ORDER_REQUIRED_FIELDS

logger = logging.getLogger(__name__)

"""
Проверка заказа на качество данных
"""


def validate_order(
        order: Dict[str, Any],
        user_ids: set,
        line_num: Optional[int] = None
) -> Tuple[bool, List[str]]:
    errors = []
    location = f"в строке {line_num}" if line_num else ""

    if not isinstance(order, dict):
        errors.append(
            f"Заказ {location} не является словарем: {type(order).__name__}"
        )
        return False, errors
    
    order_missing = ORDER_REQUIRED_FIELDS - order.keys()
    if order_missing:
        errors.append(f"В заказе {location} пропущены поля: {order_missing}")
        return False, errors
    
    try:
        amount = order["amount"]
        if not isinstance(amount, (int, float)):
            errors.append(
                f"Заказ {location}: amount должен быть числом, "
                f"получен {type(amount).__name__}"
            )
        elif amount <= 0:
            errors.append(
                f"Заказ {location}: amount должен быть > 0, получено {amount}"
            )
    except Exception as e:
        errors.append(f"Заказ {location}: ошибка проверки amount: {e}")

    user_id = order.get("user_id")
    if user_id is not None and user_id not in user_ids:
        errors.append(
            f"Заказ {location}: пользователь с id={user_id} не существует"
        )

    for field in ORDER_REQUIRED_FIELDS:
        if order.get(field) is None:
            errors.append(f"Заказ {location}: поле '{field}' равно null")

    created_at = order.get("created_at")
    if created_at is not None and not isinstance(created_at, str):
        errors.append(
            f"Заказ {location}: created_at должен быть строкой, "
            f"получен {type(created_at).__name__}"
        )
    elif created_at is not None and not created_at.strip():
        errors.append(f"Заказ {location}: created_at не может быть пустым")

    return len(errors) == 0, errors


"""
Фильтрация заказов по watermark (created_at > watermark)
"""


def filter_by_watermark(
    orders: List[Dict[str, Any]],
    watermark: str
) -> List[Dict[str, Any]]:
    logger.info(f"Фильтрация заказов по watermark: {watermark}")

    filtered = []
    skipped = 0

    for order in orders:
        created_at = order.get("created_at", "")
        if created_at > watermark:
            filtered.append(order)
        else:
            skipped += 1
            logger.debug(
                f"Пропущен заказ {order.get('order_id')}: "
                f"created_at={created_at} <= {watermark}"
            )
    
    logger.info(
        f"Отфильтровано {len(filtered)} заказов из {len(orders)} "
        f"пропущено {skipped}"
    )
    return filtered


# Вспомогательные функции
"""
Извлечение множества ID пользователей
"""
def get_user_ids(users: List[Dict[str, Any]]) -> set:
    return {user.get("id") for user in users if user.get("id") is not None}