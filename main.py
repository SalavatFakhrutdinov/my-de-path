"""
Требования:
* убрать пробелы по краям
* привести к lowercase
* заменить двойные пробелы на один
"""

def normalize_name(name: str) -> str:
    cleaned_str = " ".join(input_str.split()).lower()

input_str = input()