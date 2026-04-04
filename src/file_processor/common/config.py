import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


"""
Класс для работы с конфигурацией
"""


class Config:
    """
    Инициализация конфигурации
    """

    def __init__(self, config_path: Optional[Path] = None, env: Optional[str] = None):
        self.env = env or os.getenv("APP_ENV", "development")
        self.config_path = config_path or self._get_default_config_path()
        self._config = self._load_config()

    """
    Возвращает путь к конфигурационному файлу по умолчанию
    """

    def _get_default_config_path(self) -> Path:
        current_dir = Path(__file__).parent
        project_root = current_dir.parent.parent
        return project_root / "configs" / "config.yaml"

    """
    Загружает конфигурацию из YAML файла
    """

    def _load_config(self) -> Dict[str, Any]:
        try:
            with open(self.config_path, "r", encoding="utf-8") as file:
                full_config = yaml.safe_load(file)

            env_config = full_config.get(self.env, full_config.get("default", {}))

            logger.info(f"Загружена конфигурация для окружения: {self.env}")
            return env_config

        except FileNotFoundError:
            logger.warning(
                f"Файл конфигурации не найден: {self.config_path}, используются значения по умолчанию"
            )
            return self._get_defaults()
        except yaml.YAMLError as e:
            logger.error(
                f"Ошибка парсинга конфигурации: {e}, используются значения по умолчанию"
            )
            return self._get_defaults()

    """
    Возвращает значения по умолчанию
    """

    def _get_defaults(self) -> Dict[str, Any]:
        return {
            "users_file": "data/users.json",
            "orders_file": "data/orders.json",
            "output_file": "data/output/orders_enriched.csv",
            "watermark": "2026-03-03",
            "retry": {"attempts": 3, "delay": 1.0, "backoff": 2.0},
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s | %(levelname)8s |%(name)s | %(message)s",
            },
        }

    """
    Получает значение по ключу (поддерживает точечную нотацию)
    """

    def get(self, key: str, default: Any = None) -> Any:
        keys = key.split(".")
        value = self._config

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        return value

    """
    Переопределяет watermark (для CLI)
    """

    def set_watermark(self, watermark: str) -> None:
        self._config["watermark"] = watermark
        logger.info(f"Watermark установлен: {watermark}")

    # Свойства для фильтрации пользователей (обратная совместимость)
    """
    Минимальный возраст для фильтрации
    """

    @property
    def min_age(self) -> int:
        return self.get("min_age", 18)

    """
    Путь к входному файлу
    """

    @property
    def input_file(self) -> str:
        return self.get("input_file", "data/users.json")

    """
    Путь к выходному файлу
    """

    @property
    def output_file(self) -> str:
        return self.get("output_file", "data/processed/users.csv")

    # Свойства для ETL-pipeline
    """
    Путь к файлу пользователей
    """

    @property
    def users_file(self) -> str:
        return self.get("users_file", "data/users.json")

    """
    Путь к файлу заказов
    """

    @property
    def orders_file(self) -> str:
        return self.get("orders_file", "data/orders.json")

    """
    Путь к выходному файлу с обогащенными заказами
    """

    @property
    def enriched_output_file(self) -> str:
        return self.get("output_file", "data/output/orders_enriched.csv")

    """
    Watermark для инкрементальной обработки
    """

    @property
    def watermark(self) -> str:
        return self.get("watermark", "2026-03-03")

    """
    Удобный доступ к настройкам retry
    """

    @property
    def retry_settings(self) -> Dict[str, Any]:
        return {
            "max_attempts": self.get("retry.attempts", 3),
            "delay": self.get("retry.delay", 1.0),
            "backoff": self.get("retry.backoff", 2.0),
        }

    # Конфигурация для PostgreSQL

    """
    Хост
    """

    @property
    def pg_host(self) -> str:
        return self.get("postgres.host", "localhost")

    """
    Порт
    """

    @property
    def pg_port(self) -> int:
        return self.get("postgres.port", 5432)

    """
    Название БД
    """

    @property
    def pg_database(self) -> str:
        return self.get("postgres.database", "etl_db")

    """
    Пользователь
    """

    @property
    def pg_user(self) -> str:
        return self.get("postgres.user", "postgres")

    """
    Пароль
    """

    @property
    def pg_password(self) -> str:
        password = self.get("postgres.password", "")

        if password.startswith("${") and password.endswith("}"):
            import os

            env_var = password[2:-1]
            return os.getenv(env_var, "")
        return password

    """
    Минимальное количество подключений
    """

    @property
    def pg_min_connections(self) -> int:
        return self.get("postgres.pool.min_connections", 1)

    """
    Максимальное количество подключений
    """

    @property
    def pg_max_connections(self) -> int:
        return self.get("postgres.pool.max_connections", 10)

    """
    Строка подключения к БД
    """

    @property
    def pg_connection_string(self) -> str:
        return {
            f"host={self.pg_host} "
            f"port={self.pg_port} "
            f"dbname={self.pg_database} "
            f"user={self.pg_user} "
            f"password={self.pg_password}"
        }


"""
Загружает конфигурацию
"""


def load_config(config_path: Optional[str] = None, env: Optional[str] = None) -> Config:
    path = Path(config_path) if config_path else None
    return Config(config_path=path, env=env)


# Глобальный экземпляр конфигурации
_config: Optional[Config] = None


"""
Возвращает глобальный экземпляр конфигурации
"""


def get_config() -> Config:
    global _config
    if _config is None:
        _config = load_config()
    return _config
