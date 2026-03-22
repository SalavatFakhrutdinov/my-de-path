"""
ETL-pipeline для обогащения заказов данными пользователей
"""
import argparse
import logging
import sys
import json
from typing import NoReturn, List, Dict, Any

from file_processor.common.logging_config import configure_logging
from file_processor.common.config import load_config
from file_processor.common.writer import write_csv
from file_processor.common.constants import EXIT_SUCCESS, EXIT_FAILURE, EXIT_INTERRUPT
from file_processor.common.retry import RETRYABLE_EXCEPTIONS
from file_processor.orders_enrichment.validator import (
    filter_by_watermark, 
    validate_order
)
from file_processor.orders_enrichment.extractor import(
    extract_users,
    extract_orders,
    get_user_ids
)
from file_processor.orders_enrichment.transformer import(
    join_users_orders,
    enrich_orders_with_user_data
)

logger = logging.getLogger(__name__)


"""
Парсит аргументы командной строки
"""
def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ETL-pipeline для обогащения заказов данными пользователей",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  python -m file_processor.orders_enrichment.main
  python -m file_processor.orders_enrichment.main --config configs/config.yaml
  python -m file_processor.orders_enrichment.main --env production
  python -m file_processor.orders_enrichment.main --verbose
  python -m file_processor.orders_enrichment.main --watermark "2026-03-10"
        """
    )

    parser.add_argument(
        "--config", "-c",
        help="Путь к YAML файлу конфигурации"
    )

    parser.add_argument(
        "--env",
        choices=["development", "production", "testing"],
        default="development",
        help="Окружение (по умолчанию: development)"
    )

    parser.add_argument(
        "--verbose", "-v", 
        action="store_true", 
        help="Включить подробный режим (DEBUG уровень)"
    )

    parser.add_argument(
        "--watermark",
        help="Переопределить watermark (формат YYYY-MM-DD)"
    )

    return parser.parse_args()


"""
Вывод статистики выполнения
"""


def print_stats(stats: dict) -> None:
    logger.info("=" * 50)
    logger.info("СТАТИСТИКА ПАЙПЛАЙНА")
    logger.info("=" * 50)
    logger.info(f"Пользователей прочитано: {stats.get('users_read', 0)}")
    logger.info(f"Заказов прочитано: {stats.get('orders_read', 0)}")
    logger.info(f"Заказов после фильтрации: {stats.get('orders_filtered', 0)}")
    logger.info(f"Валидных заказов: {stats.get('orders_valid', 0)}")
    logger.info(f"Невалидных заказов: {stats.get('orders_invalid', 0)}")
    logger.info(f"Обогащенных заказов: {stats.get('orders_enriched', 0)}")
    logger.info("=" * 50)


"""
Запуск ETL-pipeline
"""


def run_etl_pipeline(config) -> dict:
    stats = {
        "users_read": 0,
        "orders_read": 0,
        "orders_filtered": 0,
        "orders_valid": 0,
        "orders_invalid": 0,
        "orders_enriched": 0
    }

    # Шаг 1 - EXTRACT
    logger.info("=" * 50)
    logger.info("Шаг 1 - EXTRACT")
    logger.info("=" * 50)

    users = extract_users(config.users_file)
    if users is None:
        logger.error("Не удалось извлечь пользователей")
        return stats
    
    orders = extract_users(config.orders_file)
    if orders is None:
        logger.error("Не удалось извлечь заказы")
        return stats
    

    stats["users_read"] = len(users)
    stats["orders_read"] = len(orders)
    logger.info(f"Извлечено пользователей: {stats['users_read']}")
    logger.info(f"Извлечено заказов: {stats['orders_read']}")

    # Шаг 2 - INCREMENTAL FILTER (по watermark)
    logger.info("=" * 50)
    logger.info("Шаг 2 - INCREMENTAL FILTER")
    logger.info("=" * 50)
    logger.info(f"Watermark: {config.watermark}")

    filtered_orders = filter_by_watermark(orders, config.watermark)
    stats["orders_filtered"] = len(filtered_orders)

    if not filtered_orders:
        logger.info("Нет заказов после фильтрации, пайплайн завершен")
        return stats
    
    # Шаг 3 - DATA QUALITY
    logger.info("=" * 50)
    logger.info("Шаг 3 - DATA QUALITY")
    logger.info("=" * 50)

    user_ids = get_user_ids(users)
    valid_orders = []

    for i, order in enumerate(filtered_orders, 1):
        is_valid, errors = validate_order(order, user_ids, i)

        if is_valid:
            valid_orders.append(order)
            stats["orders_valid"] += 1
        else:
            stats["orders_invalid"] += 1
            logger.warning(
                f"Невалидный заказ {order.get('order_id')}: {errors}"
            )
    
    logger.info(
        f"Валидных заказов: {stats['orders_valid']}"
        f"Невалидных заказов: {stats['orders_invalid']}"
    )

    if not valid_orders:
        logger.warning("Нет валидных заказов после DQ-проверок")
        return stats
    
    # Шаг 4 - JOIN (TRANSFORM)
    logger.info("=" * 50)
    logger.info("Шаг 4 - JOIN (TRANSFORM)")
    logger.info("=" * 50)

    enriched_orders = join_users_orders(users, valid_orders)
    stats["orders_enriched"] = len(enriched_orders)
    logger.info(f"Обогащено заказов: {stats['orders_enriched']}")

    # Шаг 5 - LOAD
    logger.info("=" * 50)
    logger.info("Шаг 5 - LOAD")
    logger.info("=" * 50)

    fields = ["user_id", "name", "age", "order_id", "amount", "created_at"]
    success = write_csv(
        enriched_orders,
        config.enriched_output_file,
        fields,
        allow_empty=True
    )

    if not success:
        logger.error("Ошибка записи результата")
        return stats
    
    return stats


"""

"""


def main() -> NoReturn:
    args = parse_arguments()

    config = load_config(
        config_path=args.config,
        env=args.env
    )

    log_level = "DEBUG" if args.verbose else config.get("logging.level", "INFO")
    configure_logging(
        level=log_level,
        verbose=args.verbose
    )

    if args.watermark:
        config.set_watermark(args.watermark)

    logger.info("=" * 50)
    logger.info("ЗАПУСК ETL-ПАЙПЛАЙНА")
    logger.info("=" * 50)
    logger.info(f"Окружение: {args.env}")
    logger.info(f"Watermark: {config.watermark}")
    logger.info(f"Входной файл пользователей: {config.users_file}")
    logger.info(f"Входной файл заказов: {config.orders_file}")
    logger.info(f"Выходной файл: {config.enriched_output_file}")

    try:
        stats = run_etl_pipeline(config)
        print_stats(stats)

        if stats.get("orders_enriched", 0) > 0:
            logger.info("ETL-пайплайн завершен успешно")
        elif stats.get("orders_filtered", 0) == 0:
            logger.info("Нет новых данных для обработки")
        else:
            logger.warning("ETL-пайплайн завершен с предупреждениями")

        sys.exit(EXIT_SUCCESS)

    except KeyboardInterrupt:
        logger.warning("Программа прервана пользователем")
        sys.exit(EXIT_INTERRUPT)
    except Exception as e:
        logger.exception(f"Критическая ошибка: {e}")
        sys.exit(EXIT_FAILURE)


if __name__ == "__main__":
    main()