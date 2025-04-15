"""Orchestrates the extension retrieval process"""

import time
from dotenv import load_dotenv

from setup import configure_logger, setup_db
from fetch_extensions import fetch_extensions_and_publishers


def main() -> None:
    """Executes the entire extension retrieval process"""

    start_time = time.time()

    # Setup
    load_dotenv()
    logger = configure_logger()
    logger.info("main: Starting VSCavator fetch script")

    if not setup_db(logger):
        logger.error("main: Failed to setup the database")
        return

    # Fetch data from VSCode Marketplace
    if not fetch_extensions_and_publishers(logger):
        logger.error("main: Failed to fetch data from the VSCode Marketplace")
        return

    logger.info("main: Finished VSCavator fetch script")

    end_time = time.time()
    duration_minutes = (end_time - start_time) / 60
    logger.info("main: VSCavator fetch took %.2f minutes", duration_minutes)


if __name__ == "__main__":
    main()
