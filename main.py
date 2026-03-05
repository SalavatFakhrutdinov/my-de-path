from utils import normalize_name

def main():
    try:
        user_input = input('Введите строку: ')

        if user_input and user_input.strip():
            normalized = normalize_name(user_input)
            print(f"Нормализованная строка: '{normalized}'")
        else:
            print('Ожидалась строка, получены только пробелы')
    except KeyboardInterrupt:
        print('\nИнициализировано завершение программы')

if __name__ == "__main__":
    main()
