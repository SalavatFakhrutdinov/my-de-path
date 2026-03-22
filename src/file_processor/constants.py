# Константы для валидации (validators.py)
USER_REQUIRED_FIELDS = {"id", "name", "age"}
ORDER_REQUIRED_FIELDS = {"order_id", "user_id", "amount", "created_at"}
MIN_AGE = 0
MAX_AGE = 120

# Константы для основного запуска (main.py)
DEFAULT_MIN_AGE = 18
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_INTERRUPT = 130
