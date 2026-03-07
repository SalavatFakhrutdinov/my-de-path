from typing import List, Dict, Set
import logging


logger = logging.getLogger(__name__)

"""
Фильтр пользователей по возрасту (старше 18 лет)
"""
def get_adult_users(users: List[Dict[str]], age: int) -> List[Dict[str]]:
    logger.debug(f"Фильтрация взрослых из {len(users)} пользователей")
    adults = [user for user in users if user["age"] >= 18]
    logger.info(f"Обнаружено {len(adults)} взрослых пользователей")
    return adults


"""
Вовзращает список имен
"""
def get_names(users: List[Dict[str]]) -> List[str]:
    logger.debug(f"Извлечение списка имен")
    names = [user["name"] for user in users]
    logger.info(f"Извлечено имен в количестве {len(names)}")
    return names


"""
Сортирует пользователей по возрасту
"""
def sort_by_age(users: List[Dict[str]], reverse: bool = False) -> List[Dict[str]]:
    order = "убывания" if reverse else "возрастания"
    logger.debug(f"Сортировка {len(users)} пользователей по возрасту"
                 "в порядке {order}")
    sorted_users = sorted(users, key=lambda x: x["age"], reverse=reverse)
    if logger.isEnabledFor(logging.DEBUG):
        ages = [user["age"] for user in sorted_users]
        logger.debug(f"Возраста после сортировки: {ages}")
    return sorted_users


"""
Построить словарь типа {id: name}
"""
def build_dict(users: List[Dict[str]]) -> Dict[int, str]:
    logger.debug(f"Построение словаря id: name для {len(users)} пользователей")
    users_dict = {user["id"]: user["name"] for user in users}
    logger.info(f"Построен словарь из {len(users_dict)} вхождений")
    return users_dict


"""
Получает множество уникальных возрастов
"""
def get_unique_ages(users: List[Dict[str]]) -> Set[int]:
    logger.debug(f"Получение уникального множества возрастов")
    unique_ages = {user["age"] for user in users}
    logger.info(f"Получено множество возрастов размером {len(unique_ages)}:"
                f"{sorted(unique_ages)}")
    return unique_ages


"""
Группирует пользователей по возрасту
"""
def grouped_by_age(users: List[Dict[str]]) -> Dict[int, List[Dict[str]]]:
    logger.debug(f"Группировка {len(users)} пользователей по возрасту")
    result = {}
    for user in users:
        age = user["age"]
        if age not in result:
            result[age] = []
            logger.debug(f"Создана группа для возраста {age}")
        result[age].append(user)
    logger.info(f"Создано {len(result)} групп возрастов")
    for age, group in sorted(result.items()):
        logger.debug(f"Возраст {age}: {len(group)} пользователей")

    return(result)


"""
Применение всех функций обработки
"""
def process_users(users: List[Dict[str]]) -> Dict[str]:
    logger.info("Инициализация процесса обработки пользователей")

    results = {
        "Взрослые": get_adult_users(users),
        "Имена": get_names(users),
        "Сортированные по возрасту": sort_by_age(users),
        "Сортированные по возрасту в обратном порядке": sort_by_age(users, reverse=True),
        "Словарь пользователей": build_dict(users),
        "Уникальные возраста": sorted(get_unique_ages(users)),
        "Сгруппированные по возрасту": {
            age: [user["name"] for user in user_list]
            for age, user_list in grouped_by_age(users).items()
        }
    }

    logger.info("Обработка пользователей успешно завершена")
    return results
