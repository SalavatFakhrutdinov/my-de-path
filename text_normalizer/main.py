from src.utils import normalize_name
from logging_config import configure_logging


def main():
    configure_logging(
        level="INFO",
        log_file="main_file.log",
        verbose=False
    )
    try:
        user_input = input("Введите строку: ")

        if user_input and user_input.strip():
            normalized = normalize_name(user_input)
            print(f"Нормализованная строка: '{normalized}'")
        else:
            print("Ожидалась строка, получены только пробелы")
    except KeyboardInterrupt:
        print("\nИнициализировано завершение программы")


if __name__ == "__main__":
    main()
