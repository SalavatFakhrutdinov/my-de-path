from utils import normalize_name

def main():
    try:
        user_input = input('Введите строку: ')

        if user_input and user_input.strip():
            normalized = normalize_name(user_input)
            print(f"Нормализованная строка: '{normalized}'")
        else:
            print('Expected a string input is empty or spaces only')
    except KeyboardInterrupt:
        print('\nProgram is finished by user')
    except Exception as err:
        print(f"Error occured: {err}")

if __name__ == "__main__":
    main()