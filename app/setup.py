"""Setup scripts"""

import os
import logging
from logging import Logger
import psycopg2

from util import connect_to_database

CREATE_EXTENSIONS_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS extensions (
        extension_id VARCHAR(255) PRIMARY KEY NOT NULL,
        extension_name VARCHAR(255) NOT NULL,
        display_name VARCHAR(255) NOT NULL,
        flags TEXT,
        last_updated DATE NOT NULL,
        published_date DATE NOT NULL,
        release_date DATE NOT NULL,
        short_description TEXT NOT NULL,
        latest_release_version VARCHAR(255) NOT NULL,
        latest_release_asset_uri TEXT NOT NULL,
        publisher_id VARCHAR(255) NOT NULL,
        extension_identifier VARCHAR(255) NOT NULL,
        github_url TEXT NOT NULL,
        FOREIGN KEY (publisher_id) REFERENCES publishers (publisher_id) ON DELETE CASCADE
    );
"""
CREATE_PUBLISHERS_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS publishers (
        publisher_id VARCHAR(255) PRIMARY KEY NOT NULL,
        publisher_name VARCHAR(255) NOT NULL,
        display_name VARCHAR(255) NOT NULL,
        flags TEXT,
        domain VARCHAR(255),
        is_domain_verified BOOLEAN NOT NULL
    );
"""
CREATE_RELEASES_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS releases (
        release_id VARCHAR(255) PRIMARY KEY NOT NULL,
        extension_id VARCHAR(255) NOT NULL,
        version VARCHAR(255) NOT NULL,
        last_updated DATE,
        flags TEXT,
        uploaded_to_s3 BOOLEAN NOT NULL DEFAULT FALSE,
        FOREIGN KEY (extension_id) REFERENCES extensions (extension_id) ON DELETE CASCADE
    );
"""
CREATE_REVIEWS_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS reviews (
        review_id BIGINT PRIMARY KEY NOT NULL,
        extension_id VARCHAR(255) NOT NULL,
        user_id VARCHAR(255) NOT NULL,
        user_display_name VARCHAR(255) NOT NULL,
        updated_date DATE NOT NULL,
        rating INT NOT NULL,
        text TEXT NOT NULL,
        product_version VARCHAR(255) NOT NULL,
        FOREIGN KEY (extension_id) REFERENCES extensions (extension_id) ON DELETE CASCADE
    );
"""
CREATE_STATISTICS_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS statistics (
        statistic_id VARCHAR(255) PRIMARY KEY NOT NULL,
        extension_id VARCHAR(255) NOT NULL,
        insertion_date DATE NOT NULL,
        install BIGINT NOT NULL,
        average_rating FLOAT NOT NULL,
        rating_count BIGINT NOT NULL,
        trending_daily FLOAT NOT NULL,
        trending_monthly FLOAT NOT NULL,
        trending_weekly FLOAT NOT NULL,
        update_count BIGINT NOT NULL,
        weighted_rating FLOAT NOT NULL,
        download_count BIGINT NOT NULL,
        FOREIGN KEY (extension_id) REFERENCES extensions (extension_id) ON DELETE CASCADE
    );
"""


def create_table(
    logger: Logger,
    connection: psycopg2.extensions.connection,
    table_name: str,
    create_table_query: str,
) -> bool:
    """Executes the create table query for the given table"""

    if connection is None:
        logger.error(
            "create_table: Failed to create %s table: no database connection",
            table_name,
        )
        return False

    cursor = connection.cursor()
    cursor.execute(create_table_query)
    connection.commit()
    cursor.close()

    logger.info("create_table: Created %s table", table_name)
    return True


def setup_db(logger: Logger) -> bool:
    """Creates the publishers, extensions, releases, and reviews tables"""

    connection = connect_to_database(logger)
    if not connection:
        logger.error("setup_db: Failed to connect to database")
        return False

    publishers = create_table(
        logger, connection, "publishers", CREATE_PUBLISHERS_TABLE_QUERY
    )
    extensions = create_table(
        logger, connection, "extensions", CREATE_EXTENSIONS_TABLE_QUERY
    )
    releases = create_table(logger, connection, "releases", CREATE_RELEASES_TABLE_QUERY)
    reviews = create_table(logger, connection, "reviews", CREATE_REVIEWS_TABLE_QUERY)
    statistics = create_table(
        logger, connection, "statistics", CREATE_STATISTICS_TABLE_QUERY
    )

    if (
        not publishers
        or not extensions
        or not releases
        or not reviews
        or not statistics
    ):
        logger.error("setup_db: Failed to create the tables")
        connection.close()
        return False

    logger.info("setup_db: Created the tables")
    return True


def configure_logger() -> Logger:
    """Configures the logger for the application"""

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logger = logging.getLogger(os.getenv("LOGGER_NAME"))
    return logger
