"""
vscavator.py orchestrates the extension retrieval process
"""

import os
import logging
import logging.config
from logging import Logger
from dotenv import load_dotenv

from setup_db import setup_db
from fetch_extensions import fetch_extensions_and_publishers
from fetch_releases import fetch_releases
from fetch_reviews import fetch_reviews
from upload_releases import upload_releases
from validate_data import validate_data


def configure_logger() -> Logger:
    """
    configure_logger configures the logger for the application
    """

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logger = logging.getLogger(os.getenv("LOGGER_NAME"))
    return logger


def main() -> None:
    """
    main handles the entire extension retrieval process
    """

    load_dotenv()
    logger = configure_logger()

    setup_db(logger)
    fetch_extensions_and_publishers(logger)
    fetch_releases(logger)
    fetch_reviews(logger)
    upload_releases(logger)
    validate_data(logger)


if __name__ == "__main__":
    main()
