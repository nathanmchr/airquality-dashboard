# logger_config.py
import logging
import sys
from logging.handlers import RotatingFileHandler
import os

# Ensure log directory exists
os.makedirs("logs", exist_ok=True)

LOG_FILENAME = "logs/etl.log"

formatter = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

file_handler = RotatingFileHandler(
    LOG_FILENAME, maxBytes=5_000_000, backupCount=3, encoding="utf-8"
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Configure the root logger
logging.basicConfig(level=logging.INFO, handlers=[console_handler, file_handler])


def get_logger(name: str) -> logging.Logger:
    """Return a logger with the shared configuration."""
    return logging.getLogger(name)
