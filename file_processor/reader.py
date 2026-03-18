import json
import logging
import time
from typing import List, Dict, Any, Optional, Iterator
from functools import wraps

logger = logging.getLogger(__name__)

RETRYABLE_EXCEPTIONS = (
    FileNotFoundError,
    PermissionError,
    ConnectionError,
    TimeoutError,
    OSError
)


"""
Декоратор для повторных попыток при временных ошибках
"""


def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except RETRYABLE_EXCEPTIONS as e:
                    last_exception = e
                    if attempt == max_attempts:
                        logger.error(f"Ошибка после {max_attempts} попыток: {e}")
                        raise

                    logger.warning(
                        f"Попытка {attempt}/{max_attempts} не удалась: {e}. "
                        f"Повтор через {current_delay:.1f}сек..."
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff
                except Exception as e:
                    logger.error(f"Ошибка, не подлежащая retry: {e}")
                    raise
            
            raise last_exception or RuntimeError("Неожиданный выход попытки retry")
        
        return wrapper
    return decorator


"""
Открытие файла с поддержкой retry
"""


@retry(max_attempts=3, delay=1.0)
def _open_file_with_retry(filepath: str):
    return open(filepath, 'r', encoding='utf-8')


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
