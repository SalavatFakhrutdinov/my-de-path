"""
CLI утилита для фильтрации пользователей по возрасту
и сохранения в CSV
"""
import argparse
import logging
import sys
from typing import NoReturn

from logging_config import configure_logging
from reader import read_json
from validator import validate_users, filter_adults, sort_by_age
from writer import write_csv

