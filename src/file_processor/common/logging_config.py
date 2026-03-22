import logging
import sys
from typing import Optional


def configure_logging(
    level: str = "INFO", log_file: Optional[str] = None, verbose: bool = False
) -> None:

    handlers = [logging.StreamHandler(sys.stdout)]

    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    if verbose:
        level = "DEBUG"

    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s | %(levelname)8s |%(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )
