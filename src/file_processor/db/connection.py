import logging
from contextlib import contextmanager
from typing import Optional, Generator

import psycopg2
from psycopg2 import pool, sql, OperationalError
from psycopg2.extras import execute_values

from file_processor.common.config import get_config

logger = logging.getLogger(__name__)

_connection_pool: Optional[pool.SimpleConnectionPool] = None


"""
Создание или возврат существующего пула соединений
"""


def get_connection_pool():
    global _connection_pool

    if _connection_pool is None:
        config = get_config()

        logger.info("Создание пула соединений PostgreSQL...")

        _connection_pool = pool.SimpleConnectionPool(
            minconn=config.pg_min_connections,
            maxconn=config.pg_max_connections,
            host=config.pg_host,
            port=config.pg_port,
            dbname=config.pg_database,
            user=config.pg_user,
            password=config.pg_password,
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=5,
            keepalives_interval=2,
            keepalives_count=2,
        )

        logger.info(
            f"Создан пул соединений: "
            f"min={config.pg_min_connections}, max={config.pg_max_connections}"
        )

    return _connection_pool


"""
Закрытие всех соединений в пуле
"""


def close_pool():
    global _connection_pool

    if _connection_pool is not None:
        logger.info("Закрытие пула соединений...")
        _connection_pool.closeall()
        _connection_pool = None
        logger.info("Пул соединений закрыт")


"""
Контекстный менеджер для получения соединения из пула
"""


@contextmanager
def get_connection() -> Generator:
    pool_instance = get_connection_pool()
    conn = pool_instance.getconn()

    try:
        yield conn
    except Exception as e:
        conn.rollback()
        logger.error(f"Откат транзакции: {e}")
        raise
    finally:
        pool_instance.putconn(conn)


"""
Проверка подключения к БД
"""


def test_connection() -> bool:
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                if result and result[0] == 1:
                    logger.info("Тест соединения с БД успешно завершен")
                    return True
        return False
    except OperationalError as e:
        logger.error("Тест подключения к БД провалился: {e}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка во время теста подключения: {e}")
        return False
