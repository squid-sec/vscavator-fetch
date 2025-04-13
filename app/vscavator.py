"""Orchestrates the extension retrieval process"""

from dotenv import load_dotenv

from setup import configure_logger, setup_db
from fetch_extensions import fetch_extensions_and_publishers


def main() -> None:
    """Executes the entire extension retrieval process"""

    # Setup
    load_dotenv()
    logger = configure_logger()
    if not setup_db(logger):
        logger.error("main: Failed to setup the database")
        return

    # Fetch data from VSCode Marketplace
    if not fetch_extensions_and_publishers(logger):
        logger.error("main: Failed to fetch data from the VSCode Marketplace")
        return


if __name__ == "__main__":
    main()
