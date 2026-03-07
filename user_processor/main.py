import argparse
import sys
import logging
import json
from typing import NoReturn, Dict, List

from logging_config import configure_logging
from utils import process_users

DEFAULT_USERS = [
    {"id": 1, "name": "Alice", "age": 25},
    {"id": 2, "name": "Bob", "age": 17},
    {"id": 3, "name": "Charlie", "age": 32},
    {"id": 4, "name": "Diana", "age": 25},
]

EXIT_SUCCESS = 0
EXIT_INTERRUPT = 130

logger = logging.getLogger(__name__)

"""
Парсинг аргументов командной строки
"""


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Обработка данных пользователей",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
    python main.py                    # обычный запуск
    python main.py -v                  # подробный режим
    python main.py --log-file app.log   # запись логов в файл
    python main.py -v --log-file debug.log  # отладка в файл
        """,
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Включить подробный режим (DEBUG уровень)",
    )

    parser.add_argument(
        "--log-file", type=str, help="Путь к файлу для сохранения логов"
    )

    parser.add_argument(
        "--input-file",
        type=str,
        help="Путь к JSON файлу с пользователями (если не указан, используются"
        "тестовые данные)",
    )


"""
Загрузка пользователей из JSON файла
"""


def load_users_from_file(filepath: str) -> list:
    logger.info(f"Загрузка пользователей из файла: {filepath}")

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            logger.info(f"Успешно загружен {len(data)} пользователей")
            return data
    except FileNotFoundError:
        logger.error(f"Файл {filepath} не найден")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Некорректный JSON в файле {filepath}: {e}")
        return None


"""
Проверка корректности данных пользователей
Вовзращает True в случае корректности данных, иначе False
"""


def validate_users(users: List[Dict[str]]) -> bool:
    if not isinstance(users, list):
        logger.error(f"Ожидался список, получен {type(users)}")
        return False

    required_fields = {"id", "name", "age"}
    valid = True

    for i, user in enumerate(users):
        if not isinstance(user, dict):
            logger.error(f"Пользователь {i} не словарь: {type(user)}")
            valid = False
            continue

        missing = required_fields - user.keys()
        if missing:
            logger.error(
                f"У пользователя {i} (id={user.get('id', 'unknown')})"
                "не хватает полей: {missing}"
            )
            valid = False

    return valid


"""
Форматирует результаты для вывода
"""


def format_results(results: Dict[str]) -> str:
    if "error" in results:
        return f"\n ОШИБКА: {results['error']}"

    lines = []
    lines.append("\n" + "=" * 60)
    lines.append("РЕЗУЛЬТАТЫ ОБРАБОТКИ ПОЛЬЗОВАТЕЛЕЙ")
    lines.append("=" * 60)

    for key, value in results.items():
        lines.append(f"\n{key.replace('_', ' ').title()}")

        if isinstance(value, list):
            if not value:
                lines.append("  • (пусто)")
            for item in value:
                if isinstance(item, dict):
                    lines.append(
                        f"ID:{item.get('id', '?')} {item.get('name', '?')} ({item.get('age', '?')})"
                    )
                else:
                    lines.append(f"  • {item}")
        elif isinstance(value, dict):
            for k, v in sorted(value.items()):
                lines.append(f"  • {k}: {v}")
        else:
            lines.append(f"  {value}")
    return "\n".join(lines)


"""
Запуск логики приложения
"""


def run_application(args: argparse.Namespace) -> int:
    logger.info("=" * 50)
    logger.info("ИНИЦИАЛИЗАЦИЯ ОБРАБОТКИ ДАННЫХ ПОЛЬЗОВАТЕЛЕЙ")
    logger.info("=" * 50)

    users = None

    if args.input_file:
        logger.info(f"Загрузка пользователей из файла: {args.input_file}")
        users = load_users_from_file(args.input_file)
        if users is None:
            return 1
    else:
        logger.info("Использование тестовых данных")
        users = DEFAULT_USERS.copy()

    logger.info(f"Загружено {len(users)} пользователей для обработки")

    if not validate_users(users):
        return 1

    logger.info("Старт блока обработки данных")
    results = process_users(users)

    output = format_results(results)
    print(output)

    logger.info("Обработка успешно завершена")
    return 0


def main() -> NoReturn:
    args = parse_arguments()

    configure_logging(
        level="DEBUG" if args.verbose else "INFO",
        log_file=args.log_file,
        verbose=args.verbose,
    )

    try:
        exit_code = run_application(args)
        sys.exit(exit_code)

    except KeyboardInterrupt:
        logger.warning("Завершение обработки пользователем")
        sys.exit(EXIT_INTERRUPT)


if __name__ == "__main__":
    main()
