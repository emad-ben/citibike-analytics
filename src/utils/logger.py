import logging
import os
from datetime import datetime
from pathlib import Path

def setup_logger(name, log_directory="logs"):
    """
    create and configure logger object for calling module

    -- arguments --
        name: parameter used to instantiate the logger object
        log_directory: root directory for storing log files
    
    -- return --
        logger object
    """

    Path(log_directory).mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # avoid duplicate loggers if logger already exists
    if logger.handlers:
        return logger

    simple_formatter = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S"
    )

    detailed_formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s\n%(funcName)s - line %(lineno)d - %(message)s\n----------------------",
        datefmt="%m-%d-%Y %H:%M:%S"
    )

    # file handler
    today = datetime.now().strftime("%m-%d-%Y")
    log_file = os.path.join(log_directory, name, f"{name}-{today}.log")
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)

    # console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)

    # attach the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
