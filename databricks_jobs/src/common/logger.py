import logging


def get_logger(name: str) -> logging.Logger:
    """
    Create and configure an application logger.

    The logger writes to the console, so logs are captured
    automatically by Databricks notebook and job/task logs.
    """

    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(logging.INFO)

    return logger