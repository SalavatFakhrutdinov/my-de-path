"""
Модуль для интеграции с PostgreSQL
"""

from file_processor.db.connection import get_connection, get_connection_pool, close_pool
from file_processor.db.repository import (
    upsert_orders_enriched,
    get_orders_after_date,
    create_tables_if_not_exists
)

__all__ = [
    "get_connection",
    "get_connection_pool",
    "close_pool",
    "upsert_orders_enriched",
    "get_orders_after_date",
    "create_tables_if_not_exists",
]