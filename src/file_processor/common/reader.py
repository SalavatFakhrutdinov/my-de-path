import json
import logging
from typing import List, Dict, Any, Optional, Iterator

from file_processor.common.retry import retry, RETRYABLE_EXCEPTIONS

logger = logging.getLogger(__name__)


"""
Открытие файла с поддержкой retry
"""


@retry(max_attempts=3, delay=1.0)
def _open_file_with_retry(filepath: str):
    return open(filepath, "r", encoding="utf-8")


"""
Потоковое чтение JSON файла с поддержкой retry при открытии
"""


def read_json_streaming(filepath: str) -> Iterator[Dict[str, Any]]:
    logger.info(f"Потоковое чтение JSON файла {filepath}")

    try:
        with _open_file_with_retry(filepath) as file:
            first_char = file.read(1)
            file.seek(0)

            if first_char == "[":
                logger.debug("Обнаружен формат JSON массив")
                yield from _read_json_array_streaming(file)
            else:
                logger.debug("Обнаружен формат JSON строковый")
                yield from _read_jsonl_streaming(file)

    except RETRYABLE_EXCEPTIONS as e:
        logger.error(f"Ошибка открытия файла после попыток: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка парсинга JSON: {e}")
        raise
    except Exception as e:
        logger.exception(f"Неожиданная ошибка чтения {filepath}")
        raise


"""
Чтение формата JSON lines с нумерацией строк для логирования
"""


def _read_jsonl_streaming(file_obj) -> Iterator[Dict[str, Any]]:
    for line_num, line in enumerate(file_obj, 1):
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка парсинга JSON на строке {line_num}: {e}")
            raise


"""
Потоковое чтение JSON массива с отслеживанием позиции
"""


def _read_json_array_streaming(file_obj) -> Iterator[Dict[str, Any]]:
    file_obj.read(1)

    buffer = ""
    in_string = False
    escape = False
    depth = 1
    object_count = 0

    while True:
        char = file_obj.read(1)
        if not char:
            break

        if char == '"' and not escape:
            in_string = not in_string
            buffer += char
            continue
        elif char == "\\" and not escape:
            escape = True
            buffer += char
            continue
        else:
            escape = False

        if in_string:
            buffer += char
            continue

        if char == "{":
            depth += 1
            buffer += char
        elif char == "}":
            depth -= 1
            buffer += char
            if depth == 1:
                object_count += 1
                try:
                    yield json.loads(buffer)
                    buffer = ""
                except json.JSONDecodeError as e:
                    logger.error(f"Ошибка парсинга объекта #{object_count}: {e}")
        elif char == "," and depth == 1:
            continue
        else:
            buffer += char


"""
Чтение JSON файла как список (целиком)
"""


def read_json_as_list(filepath: str) -> list:
    logger.info(f"Чтение JSON файла целиком: {filepath}")
    return list(read_json_streaming(filepath))
