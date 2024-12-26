"""Orchestrates the extension retrieval process"""

from dotenv import load_dotenv

from setup import configure_logger, setup_db
from fetch_extensions import fetch_extensions_and_publishers
from fetch_releases import fetch_releases
from fetch_reviews import fetch_reviews
from upload_releases import upload_releases
from validate_data import validate_data


def main() -> None:
    """Executes the entire extension retrieval process"""

    # Setup
    load_dotenv()
    logger = configure_logger()
    setup_db(logger)

    # Fetch data from VSCode Marketplace
    fetch_extensions_and_publishers(logger)
    fetch_releases(logger)
    fetch_reviews(logger)
    upload_releases(logger)

    # Validate data in the database and S3
    validate_data(logger)


if __name__ == "__main__":
    main()
