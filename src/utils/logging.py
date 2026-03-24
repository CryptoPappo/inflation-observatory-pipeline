import logging
from logging import Logger
from logging.handlers import RotatingFileHandler
import os

def get_logger(name: str) -> Logger:
    """

    Create and configure a logger with a rotating file handler.

    This function returns a logger instance with:
      - INFO level logging
      - a RotatingFileHandler that writes to `logs/etl.log`
      - automatic log directory creation
      - a standard timestamped log format
      - protection against adding duplicate handlers


    Args:
        name (str): Name of the logger, typically the module name (e.g., "extract",
        "transform", "load", or "__main__").


    Returns:
        logging.Logger: Configured logger instance ready for use.


    Notes:
        The logger uses a rotating log file with:
            - max size: 5 MB
            - backup count: 2 files
        If the logger already has handlers, they are reused to avoid duplicate log
        entries when the function is called multiple times.
    """
    log_path = "logs/etl.log"
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        return logger

    handler = RotatingFileHandler(
        log_path, 
        maxBytes=5_000_000,
        backupCount=2
    )

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger
