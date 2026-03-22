import csv
import logging
import os
from typing import List, Dict, Any, Optional, Iterator

logger = logging.getLogger(__name__)


"""
Запись списка словарей в CSV файл
"""


def write_csv(
    data: List[Dict[str, Any]],
    filepath: str,
    fields: Optional[List[str]] = None,
    allow_empty: bool = True,
) -> bool:

    if not data and not allow_empty:
        logger.warning("Нет данных для записи и allow_empty=False")
        return False

    if fields is None and data:
        fields = list(data[0].keys())
    elif fields is None:
        logger.error("Невозможно прочитать CSV: нет данных и нет определенных полей")
        return False

    ensure_output_directory(filepath)

    logger.info(f"Запись {len(data)} записей в {filepath}")
    logger.debug(f"Поля: {fields}")

    try:
        with open(filepath, "w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()
            if data:
                writer.writerows(data)

        logger.info(f"Успешно записаны строки в количестве {len(data)} в {filepath}")
        return True

    except PermissionError:
        logger.error(f"Доступ запрещен: {filepath}")
        return False
    except Exception as e:
        logger.exception(f"Ошибка записи в {filepath}: {e}")
        return False


"""
Потоковая запись в CSV (для больших наборов данных)
"""


def write_csv_streaming(data_iter, filepath: str, fields: List[str]) -> bool:
    ensure_output_directory(filepath)

    logger.info(f"Запуск потоковой записи в {filepath}")
    logger.debug(f"Поля: {fields}")

    try:
        with open(filepath, "w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()

            count = 0
            for item in data_iter:
                writer.writerow(item)
                count += 1

                if count % 10000 == 0:
                    logger.debug(f"Записано {count} строк")

        logger.info(f"Успешно записано {count} строк в {filepath}")
        return True

    except Exception as e:
        logger.exception(f"Ошибка потоковой записи в {filepath}: {e}")
        return False


# Вспомогательные функции
"""
Создание директории для выходного файла при отсутствии
"""


def ensure_output_directory(filepath: str) -> None:
    directory = os.path.dirname(filepath)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        logger.debug(f"Создана директория: {directory}")
