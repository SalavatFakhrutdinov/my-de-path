import argparse
import sys
from typing import NoReturn, Dict, List

from logging_config import configure_logging
from utils import process_users


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
        """
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Включить подробный режим (DEBUG уровень)'
    )

    parser.add_argument(
        '--log-file',
        type=str,
        help='Путь к файлу для сохранения логов'
    )

    parser.add_argument(
        '--input-file',
        type=str,
        help='Путь к JSON файлу с пользователями (если не указан, используются' \
        'тестовые данные)'
    )


"""
Загрузка пользователей из JSON файла
"""
def load_users_from_file(filepath: str) -> list:
    import json
    import logging

    logger = logging.getLogger(__name__)

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Файл {filepath} не найден")
        sys.exit(1)
    except json.JSONDecodeError as e:
        logger.error(f"Некорректный JSON в файле {filepath}: {e}")
        sys.exit(1)


"""
Возвращает тестовые данные пользователей
"""
def get_default_users() -> list:
    return [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 17},
        {"id": 3, "name": "Charlie", "age": 32},
        {"id": 4, "name": "Diana", "age": 25}
    ]


"""
Форматирует результаты для вывода
"""
def format_results(results: Dict[str]) -> str:
    lines = []
    lines.append("\n" + "="*60)
    lines.append("РЕЗУЛЬТАТЫ ОБРАБОТКИ ПОЛЬЗОВАТЕЛЕЙ")
    lines.append("="*60)

    for key, value in results.items():
        lines.append(f"\n{key.replace('_', ' ').title()}")

        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    lines.append(f"  • ID:{item['id']} {item['name']} ({item['age']})")
                else:
                    lines.append(f"  • {item}")
        elif isinstance(value, dict):
            for k, v in sorted(value.items()):
                lines.append(f"  • {k}: {v}")
        else:
            lines.append(f"  {value}")
    return "\n".join(lines)


def main() -> NoReturn:
    args = parse_arguments()

    configure_logging(
        level="DEBUG" if args.verbose else "INFO",
        log_file=args.log_file,
        verbose=args.verbose
    )

    import logging
    logger = logging.getLogger(__name__)

    logger.info("="*50)
    logger.info("ИНИЦИАЛИЗАЦИЯ ОБРАБОТКИ ДАННЫХ ПОЛЬЗОВАТЕЛЕЙ")
    logger.info("="*50)

    try:
        if args.input_file:
            logger.info(f"Загрузка пользователей из файла: {args.input_file}")
            users = load_users_from_file(args.input_file)
        else:
            logger.info("Использование тестовых данных")
            users = get_default_users()

        logger.info(f"Загружено {len(users)} пользователей для обработки")

        required_fields = {'id', 'name', 'age'}
        for i, user in enumerate(users):
            missing = required_fields - user.keys()
            if missing:
                logger.error(f"У пользователя {i} отсутствуют поля: {missing}")
                sys.exit(1)
        
        logger.info("Старт блока обработки данных")
        results = process_users(users)

        output = format_results(results)
        print(output)

        logger.info("Обработка успешно завершена")
        sys.exit(0)
    except KeyboardInterrupt:
        logger.warning("Завершение обработки пользователем")
        sys.exit(130)


if __name__ == "__main__":
    main()