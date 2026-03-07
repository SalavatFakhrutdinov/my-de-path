import logging

logger = logging.getLogger(__name__)


"""
Очистка от пробелов, приведение к нижнему регистру
"""
def normalize_name(input_str: str) -> str:
    if not isinstance(input_str, str):
        logger.warning(
            f"Ожидалась строка, получено {type(input_str)}."
            "Вернулась пустая строка"
        )
        return ""

    if len(input_str) > 100:
        logger.info(
            f"Строка большой длины ({len(input_str)} символов)"
        )

    return " ".join(input_str.split()).lower()
