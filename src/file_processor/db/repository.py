import logging
from typing import List, Dict, Any, Optional

from psycopg2.extras import execute_values

from file_processor.db.connection import get_connection

logger = logging.getLogger(__name__)


"""
Создание таблиц в случае их отсутствия
"""


def create_tables_if_not_exists() -> None:
    logger.info("Checking/Creating database tables...")

    with get_connection() as conn:
        with conn.cursor() as cur:
            # users
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY
                    ,name TEXT
                    ,age INTEGER
                    ,updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # orders
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id INTEGER PRIMARY KEY
                    ,user_id INTEGER
                    ,amount INTEGER
                    ,created_at DATE
                    ,updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # orders_enriched
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders_enriched (
                    order_id INTEGER PRIMARY KEY
                    ,user_id INTEGER
                    ,name TEXT
                    ,age INTEGER
                    ,amount INTEGER
                    ,created_at DATE
                    ,updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # индекс для поиска по user_id

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_orders_enriched_user_id
                ON orders_enriched(user_id)
            """)
            
            # индекс для поиска по created_at

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_orders_enriched_created_at
                ON orders_enriched(created_at)
            """)
    
    logger.info("Tables created/verified successfully")


"""
Выполнение UPSERT для обогащенных заказов
"""


def upsert_orders_enriched(
        orders: List[Dict[str, Any]], batch_size: int = 1000
    ) -> int:
    if not orders:
        logger.warning("No orders to upsert")
        return 0
    
    logger.info(f"UPSERT {len(orders)} orders into orders_enriched "
                f"(batch_size={batch_size})")
    
    # Преобразование в список кортежей для execute_values

    data_tuples = [
        (
            order["order_id"],
            order["user_id"],
            order["name"],
            order["age"],
            order["amount"],
            order["created_at"],
        )
        for order in orders
    ]

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO orders_enriched (order_id, user_id, name, age, amount, created_at, updated_at)
                VALUES %s
                ON CONFLICT (order_id) DO UPDATE SET
                    user_id = EXCLUDED.user_id
                    ,name = EXCLUDED.name
                    ,age = EXCLUDED.age
                    ,created_at = EXCLUDED.created_at
                    ,updated_at = CURRENT_TIMESTAMP
                """,
                data_tuples,
                page_size=batch_size
            )

    logger.info(f"UPSERT completed: {len(orders)} records")
    return len(orders)


"""
Получение обогащенных заказов после указанной даты
"""


def get_orders_after_date(date: str) -> List[Dict[str, Any]]:
    logger.info(f"Fetching orders after date: {date}")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT order_id, user_id, name, age, amount, created_at
                FROM orders_enriched
                WHERE created_at > %s
                ORDER BY created_at DESC
                """,
                (date,)
            )

            rows = cur.fetchall()

            orders = [
                {
                    "order_id": row[0],
                    "user_id": row[1],
                    "name": row[2],
                    "age": row[3],
                    "amount": row[4],
                    "created_at": row[5],
                }
                for row in rows
            ]
    
    logger.info(f"Found {len(orders)} orders after {date}")
    return orders


"""
Получение заказов конкретного пользователя
"""


def get_orders_by_user(user_id: int) -> List[Dict[str, Any]]:
    logger.info(f"Fetching orders for user_id: {user_id}")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    order_id
                    ,user_id
                    ,name
                    ,age
                    ,amount
                    ,created_at
                FROM orders_enriched
                WHERE user_id = %s
                ORDER BY created_at DESC
                """,
                (user_id,)
            )

            rows = cur.fetchall()

            orders = [
                {
                    "order_id": row[0],
                    "user_id": row[1],
                    "name": row[2],
                    "age": row[3],
                    "amount": row[4],
                    "created_at": row[5],
                }
                for row in rows
            ]

    logger.info(f"Found {len(orders)} for user {user_id}")
    return orders


"""
Получение статистики по таблице orders_enriched
"""


def get_statistics() -> Dict[str, Any]:
    logger.info("Fetching statistics from orders_enriched")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) as total_orders
                    ,COUNT(DISTINCT user_id) as unique_users
                    ,SUM(AMOUNT) as total_amount
                    ,AVG(amount) as avg_amount
                    ,MIN(created_at) as first_order
                    ,MAX(created_at) as last_order
                FROM orders_enriched
                """
            )

            row = cur.fetchone()

            stats = {
                "total_orders": row[0] or 0,
                "unique_users": row[1] or 0,
                "total_amount": float(row[2]) if row[2] else 0,
                "avg_amount": float(row[3]) if row[3] else 0,
                "first_order": row[4] if row[4] else None,
                "last_order": row[5] if row[5] else None,
            }

    logger.info(f"Statistics: {stats['total_orders']} orders, "
                f"{stats['unique_users']} users")
    return stats