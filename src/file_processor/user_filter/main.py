"""
CLI утилита для фильтрации пользователей по возрасту
и сохранения в CSV
"""

import argparse
import logging
import sys
import json
from typing import NoReturn, List, Dict, Any

from file_processor.common.logging_config import configure_logging
from file_processor.common.config import load_config
from file_processor.common.reader import read_json_streaming
from file_processor.user_filter.validator import (
    validate_user, 
    filter_adults, 
    sort_by_age
)
from file_processor.user_filter.transformations import apply_transformations
from file_processor.common.writer import write_csv
from file_processor.common.constants import (
    DEFAULT_MIN_AGE,
    EXIT_SUCCESS,
    EXIT_FAILURE,
    EXIT_INTERRUPT,
)
from file_processor.common.retry import RETRYABLE_EXCEPTIONS

logger = logging.getLogger(__name__)


"""
Парсинг аргументов командной строки
"""


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Фильтр совершеннолетних из JSON запись в CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  python main.py --input users.json --output adults.csv
  python main.py --input users.json --output adults.csv --min-age 21
  python main.py --input users.json --output adults.csv --verbose
""",
    )

    parser.add_argument("--input", "-i", help="Введите путь к JSON файлу")

    parser.add_argument("--output", "-o", help="Вывод пути CSV файла")

    parser.add_argument(
        "--min-age",
        type=int,
        help=f"Минимальный возраст для фильтрации:" f"(переопределяет config)",
    )

    parser.add_argument("--config", "-c", help="Путь к YAML файлу конфигурации")

    parser.add_argument(
        "--env",
        choices=["development", "production", "testing"],
        default="development",
        help="Окружение (по умолчанию: development)",
    )

    parser.add_argument(
        "--verbose", "-v", 
        action="store_true", 
        help="Включить подробный режим")

    parser.add_argument("--log-file", help="Путь к файлу лога")

    return parser.parse_args()


"""
Статистика пайплайна в логи (INFO уровень)
"""


def print_pipeline_stats(stats: dict) -> None:
    logger.info("=" * 50)
    logger.info("СТАТИСТИКА ПАЙПЛАЙНА")
    logger.info("=" * 50)
    logger.info(f"Строк прочитано: {stats.get('records_read', 0)}")
    logger.info(f"Валидных строк: {stats.get('valid_records', 0)}")
    logger.info(f"Невалидных строк: {stats.get('invalid_records', 0)}")
    logger.info(f"Строк записано: {stats.get('written_records', 0)}")
    logger.info("=" * 50)


"""
Запуск ETL-пайплайна с подсчетом статистики
"""


def run_application(args: argparse.Namespace, config) -> int:
    input_file = args.input or config.input_file
    output_file = args.output or config.output_file
    min_age = args.min_age if args.min_age is not None else config.min_age

    if not input_file or not output_file:
        logger.error("Не указаны входной и выходной файлы")
        return EXIT_FAILURE

    logger.info("=" * 50)
    logger.info("ЗАПУСК ОБРАБОТКИ ПОЛЬЗОВАТЕЛЕЙ")
    logger.info("=" * 50)
    logger.info(f"Входной файл: {args.input}")
    logger.info(f"Итоговый файл: {args.output}")
    logger.info(f"Минимальный возраст: {args.min_age}")

    stats = {
        "records_read": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "written_records": 0,
    }

    all_valid_users: List[Dict[str, Any]] = []

    try:
        for line_num, user in enumerate(read_json_streaming(args.input, 1)):
            stats["records_read"] += 1

            is_valid, errors = validate_user(user, line_num)

            if is_valid:
                stats["valid_records"] += 1
                transformed = apply_transformations(user)
                all_valid_users.append(transformed)
            else:
                stats["invalid_records"] += 1
                logger.warning(
                    f"Пропуск невалидной записи в строке {line_num}: {errors}"
                )

        adult_users = filter_adults(all_valid_users, min_age)
        adult_users = sort_by_age(adult_users)
        stats["written_records"] = len(adult_users)

        logger.info(f"Запись {len(adult_users)} строк в {args.output}")
        success = write_csv(
            adult_users, args.output, fields=["id", "name", "age"], allow_empty=True
        )

        if not success:
            logger.error("Ошибка записи в итоговый файл")
            return EXIT_FAILURE

        print_pipeline_stats(stats)

        if adult_users and logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Пример трансформированного пользователя: {adult_users[0]}")
            if "age_group" in adult_users[0]:
                logger.debug(f"Возрастные группы добавлены")

    except KeyboardInterrupt:
        logger.warning("Программа завершена пользователем")
        print_pipeline_stats(stats)
        return EXIT_INTERRUPT

    except RETRYABLE_EXCEPTIONS as e:
        logger.error(f"Ошибка чтения входного файла после всех попыток: {e}")
        return EXIT_FAILURE

    except json.JSONDecodeError as e:
        logger.error(f"Ошибка парсинга JSON-файла: {e}")

    except Exception as e:
        logger.exception(f"Неожиданная ошибка пайплайна: {e}")
        return EXIT_FAILURE


def main() -> NoReturn:
    args = parse_arguments()

    config = load_config(
        config_path=args.config,
        env=args.env
    )

    log_level = "DEBUG" if args.verbose else config.get("logging.level", "INFO")
    configure_logging(
        level=log_level,
        log_file=args.log_file,
        verbose=args.verbose,
    )

    try:
        exit_code = run_application(args)
        sys.exit(EXIT_SUCCESS)

    except KeyboardInterrupt:
        logger.warning("Программа завершена пользователем")
        sys.exit(EXIT_INTERRUPT)
    except Exception as e:
        logger.exception(f"Критическая ошибка: {e}")
        sys.exit(EXIT_FAILURE)


if __name__ == "__main__":
    main()
