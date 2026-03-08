import json
import logging
from typing import List, Dict, Any, Optional, Iterator

logger = logging.getLogger(__name__)


"""
Чтение JSON файла и возврат списка пользователей
"""
def read_json(filepath: str) -> Optional[List[Dict[str, Any]]]:
    logger.info(f"Чтение JSON файла: {filepath}")

    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            data = json.load(file)

        if not isinstance(data, list):
            logger.error(f"Ожидался JSON массив," 
                         f"получен тип {type(data).__name__}")
            return None
        
        logger.info(f"Успешно загружено {len(data)} строк")
        return data
    
    except FileNotFoundError:
        logger.error(f"Файл не найден: {filepath}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Невалидный JSON в {filepath}: {e}")
        return None


"""
Потоковое чтение JSON файла и возврат объектов построчно
"""
def read_json_streaming(filepath: str) -> Iterator[Dict[str, Any]]:
    logger.info(f"Потоковое чтение JSON файла {filepath}")

    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            first_char = file.read(1)
            file.seek(0)

            if first_char == '[':
                logger.debug("Обнаружен формат JSON массив")
                yield from _read_json_array_streaming(file)
            else:
                logger.debug("Обнаружен формат JSON строковый")
                yield from _read_jsonl_streaming(file)
    
    except FileNotFoundError:
        logger.error(f"Файл не найден: {filepath}")
        return


"""
Чтение формата JSON lines
"""
def _read_jsonl_streaming(file_obj) -> Iterator[Dict[str, Any]]:
    for line_num, line in enumerate(file_obj, 1):
        line = line.strip()
        if not line:
            continue
            
        try:
            data = json.loads(line)
            yield data
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка JSON на строке {line_num}: {e}")
            continue


"""
Потоковое чтение JSON массива
"""
def _read_json_array_streaming(file_obj) -> Iterator[Dict[str, Any]]:
    file_obj.read(1)

    buffer = ""
    in_string = False
    escape = False
    depth = 1
    object_start = None

    while True:
        char = file_obj.read(1)
        if not char:
            break

        if char == '"' and not escape:
            in_string = not in_string
            buffer += char
            continue
        elif char == '\\' and not escape:
            escape = True
            buffer += char
            continue
        else:
            escape = False

        if in_string:
            buffer += char
            continue

        if char == '{':
            if depth == 1:
                object_start = len(buffer)
            depth += 1
            buffer += char
        elif char == '}':
            depth -= 1
            buffer += char
            if depth == 1:
                try:
                    obj_str = buffer[object_start:]
                    obj = json.loads(obj_str)
                    yield obj
                    buffer = ""
                    object_start = None
                except json.JSONDecodeError as e:
                    logger.error(f"Ошибка парсинга объекта: {e}")
        elif char == ',' and depth == 1:
            continue
        else:
            buffer += char