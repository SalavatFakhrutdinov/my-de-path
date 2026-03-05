import logging
from typing import Union

logger = logging.getLogger(__name__)


def normalize_name(input_str: Union[str, any]) -> str:
    if not isinstance(input_str, str):
        logger.warning(
            f"Ожидалась строка, получено {type(input_str)}.Вернулась пустая строка"
        )
        return ""

    if len(input_str) > 100:
        logger.info(
            f"Строка большой длины ({len(input_str)} символов)"
        )

    return " ".join(input_str.split()).lower()
