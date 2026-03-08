"""
CLI утилита для фильтрации пользователей по возрасту
и сохранения в CSV
"""

import argparse
import logging
import sys
from typing import NoReturn

from logging_config import configure_logging
from reader import read_json
from validator import validate_users, filter_adults, sort_by_age
from writer import write_csv
from constants import *

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

    parser.add_argument(
        "--input", "-i", required=True, help="Введите путь к JSON файлу"
    )

    parser.add_argument("--output", "-o", required=True, help="Вывод пути CSV файла")

    parser.add_argument(
        "--min-age",
        type=int,
        default=DEFAULT_MIN_AGE,
        help=f"Минимальный возраст для фильтрации:"
        f"(по умолчанию: {DEFAULT_MIN_AGE})",
    )

    parser.add_argument("--log-file", help="Путь к файлу лога")

    return parser.parse_args()


"""
Цикл обработки данных
"""


def process_data(filepath: str, min_age: int) -> tuple:
    stats = {}

    # 1 - Чтение
    data = read_json(filepath)
    if data is None:
        return None, stats

    stats["total_read"] = len(data)

    # 2 - Валидация
    valid_users, invalid_users = validate_users(data)
    stats["valid"] = len(valid_users)
    stats["invalid"] = len(invalid_users)

    if not valid_users:
        logger.warning("Нет валидных пользователей для обработки")
        return [], stats

    # 3 - Фильтрация
    adults = filter_adults(valid_users, min_age)
    stats["adults"] = len(adults)

    # 4 - Сортировка
    sorted_adults = sort_by_age(adults)

    return sorted_adults, stats


"""
Вывод сводки обработки
"""


def print_summary(stats: dict, output_file: str) -> None:
    print("\n" + "=" * 50)
    print("ИТОГИ ОБРАБОТКИ")
    print("=" * 50)
    print(f"Итого строк прочитано: {stats.get('total_read', 0)}")
    print(f"Валидных строк: {stats.get('valid', 0)}")
    print(f"Невалидных строк: {stats.get('invalid', 0)}")
    print(
        f"Совершеннолетних пользователей (>={stats.get('min_age', 18)}):"
        f"{stats.get('adults', 0)}"
    )
    print(f"Итоговый файл: {output_file}")
    print("=" * 50 + "\n")


"""
Запуск основной логики
"""


def run_apllication(args: argparse.Namespace) -> int:
    logger.info("=" * 50)
    logger.info("ЗАПУСК ОБРАБОТКИ ПОЛЬЗОВАТЕЛЕЙ")
    logger.info("=" * 50)
    logger.info(f"Входной файл: {args.input}")
    logger.info(f"Итоговый файл: {args.output}")
    logger.info(f"Минимальный возраст: {args.min_age}")

    processed_data, stats = process_data(args.input, args.min_age)

    if processed_data is None:
        logger.error("Обработка не удалась - невозможно прочитать файл")
        return EXIT_FAILURE

    stats["min_age"] = args.min_age

    if not processed_data:
        logger.warning(
            "Не обнаружено совершеннолетних пользователей" "по заданному критерию"
        )
        print_summary(stats, args.output)
        write_csv([], args.output, fields=["id", "name", "age"])
        return EXIT_SUCCESS

    success = write_csv(processed_data, args.output)

    if not success:
        logger.error("Ошибка записи в итоговый файл")
        return EXIT_FAILURE

    print_summary(stats, args.output)

    logger.info("Обработка успешно завершена")
    return EXIT_SUCCESS


def main() -> NoReturn:
    args = parse_arguments()

    try:
        exit_code = run_apllication(args)
        sys.exit(exit_code)

    except KeyboardInterrupt:
        logger.warning("Завершение обработки пользователем")
        sys.exit(EXIT_INTERRUPT)


if __name__ == "__main__":
    main()
