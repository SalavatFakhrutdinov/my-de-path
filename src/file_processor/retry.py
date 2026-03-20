import logging
import time
from functools import wraps
from typing import Type, Tuple, Optional, Any, Callable

logger = logging.getLogger(__name__)

RETRYABLE_EXCEPTIONS = (
    FileNotFoundError,
    PermissionError,
    ConnectionError,
    TimeoutError,
    OSError,
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