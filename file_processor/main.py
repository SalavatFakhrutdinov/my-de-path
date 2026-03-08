"""
CLI утилита для фильтрации пользователей по возрасту
и сохранения в CSV
"""
import argparse
import logging
import sys
from typing import NoReturn

from logging_config import configure_logging
from reader import re