"""
db.py contains database related functions
"""

import os
from logging import Logger
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

CREATE_EXTENSIONS_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS extensions (
        extension_id VARCHAR(255) PRIMARY KEY NOT NULL,
        extension_name VARCHAR(255) NOT NULL,
        display_name VARCHAR(255) NOT NULL,
        flags TEXT ARRAY,
        last_updated DATE NOT NULL,
        published_date DATE NOT NULL,
        release_date DATE NOT NULL,
        short_description TEXT NOT NULL,
        latest_release_version VARCHAR(255) NOT NULL,
        latest_release_asset_uri TEXT NOT NULL,
        publisher_id VARCHAR(255) NOT NULL,
        extension_identifier VARCHAR(255) NOT NULL,
        install BIGINT NOT NULL,
        averagerating FLOAT NOT NULL,
        ratingcount BIGINT NOT NULL,
        trendingdaily FLOAT NOT NULL,
        trendingmonthly FLOAT NOT NULL,
        trendingweekly FLOAT NOT NULL,
        updateCount BIGINT NOT NULL,
        weightedRating FLOAT NOT NULL,
        downloadCount BIGINT NOT NULL,
        FOREIGN KEY (publisher_id) REFERENCES publishers (publisher_id) ON DELETE CASCADE
    );
"""
CREATE_PUBLISHERS_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS publishers (
        publisher_id VARCHAR(255) PRIMARY KEY NOT NULL,
        publisher_name VARCHAR(255) NOT NULL,
        display_name VARCHAR(255) NOT NULL,
        flags TEXT ARRAY,
        domain VARCHAR(255),
        is_domain_verified BOOLEAN NOT NULL
    );
"""
CREATE_RELEASES_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS releases (
        release_id VARCHAR(255) PRIMARY KEY NOT NULL,
        extension_id VARCHAR(255) NOT NULL,
        version VARCHAR(255) NOT NULL,
        last_updated DATE NOT NULL,
        flags TEXT ARRAY,
        uploaded_to_s3 BOOLEAN NOT NULL DEFAULT FALSE,
        FOREIGN KEY (extension_id) REFERENCES extensions (extension_id) ON DELETE CASCADE
    );
"""


def connect_to_database(logger: Logger) -> psycopg2.extensions.connection:
    """
    connect_to_database establishes a connection to the SQL database
    """

    connection = psycopg2.connect(
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
    )

    if connection:
        logger.info(
            "connect_to_database: Connected to database %s on host %s:%s",
            os.getenv("PG_DATABASE"),
            os.getenv("PG_HOST"),
            os.getenv("PG_PORT"),
        )
        return connection

    logger.error(
        "connect_to_database: Failed to connect to database %s on host %s",
        os.getenv("PG_DATABASE"),
        os.getenv("PG_HOST"),
    )
    return None


def create_table(
    logger: Logger,
    connection: psycopg2.extensions.connection,
    table_name: str,
    create_table_query: str,
) -> None:
    """
    create_table executes the create table query for the given table
    """

    if connection is None:
        logger.error(
            "create_table: Failed to create %s table: no database connection",
            table_name,
        )
        return

    cursor = connection.cursor()
    cursor.execute(create_table_query)
    connection.commit()
    cursor.close()

    logger.info("create_table: Created %s table", table_name)


def create_all_tables(
    logger: Logger, connection: psycopg2.extensions.connection
) -> None:
    """
    create_all_tables creates the publishers, extensions, and releases tables
    """

    create_table(logger, connection, "publishers", CREATE_PUBLISHERS_TABLE_QUERY)
    create_table(logger, connection, "extensions", CREATE_EXTENSIONS_TABLE_QUERY)
    create_table(logger, connection, "releases", CREATE_RELEASES_TABLE_QUERY)


def upsert_data(
    logger: Logger,
    connection: psycopg2.extensions.connection,
    table_name: str,
    upsert_data_query: str,
    data: list,
) -> None:
    """
    upsert_data executes the upsert data query with the given data on the given table
    """

    cursor = connection.cursor()
    execute_values(cursor, upsert_data_query, data)
    connection.commit()
    cursor.close()

    logger.info(
        "upsert_data: Upserted %d rows of %s data to the database",
        len(data),
        table_name,
    )


def upsert_extensions(
    logger: Logger,
    connection: psycopg2.extensions.connection,
    extensions_df: pd.DataFrame,
    batch_size: int = 5000,
) -> None:
    """
    upsert_extensions upserts the given extensions to the database in batches.
    """

    upsert_query = """
        INSERT INTO extensions (
            extension_id, extension_name, display_name, flags, last_updated, published_date, release_date, 
            short_description, latest_release_version, latest_release_asset_uri, publisher_id, 
            extension_identifier, install, averagerating, ratingcount, trendingdaily, trendingmonthly, 
            trendingweekly, updateCount, weightedRating, downloadCount
        ) VALUES %s
        ON CONFLICT (extension_id) DO UPDATE SET
            extension_name = EXCLUDED.extension_name,
            display_name = EXCLUDED.display_name,
            flags = EXCLUDED.flags,
            last_updated = EXCLUDED.last_updated,
            published_date = EXCLUDED.published_date,
            release_date = EXCLUDED.release_date,
            short_description = EXCLUDED.short_description,
            latest_release_version = EXCLUDED.latest_release_version,
            latest_release_asset_uri = EXCLUDED.latest_release_asset_uri,
            publisher_id = EXCLUDED.publisher_id,
            extension_identifier = EXCLUDED.extension_identifier,
            install = EXCLUDED.install,
            averagerating = EXCLUDED.averagerating,
            ratingcount = EXCLUDED.ratingcount,
            trendingdaily = EXCLUDED.trendingdaily,
            trendingmonthly = EXCLUDED.trendingmonthly,
            trendingweekly = EXCLUDED.trendingweekly,
            updateCount = EXCLUDED.updateCount,
            weightedRating = EXCLUDED.weightedRating,
            downloadCount = EXCLUDED.downloadCount;
    """

    values = [
        (
            row["extension_id"],
            row["extension_name"],
            row["display_name"],
            row["flags"],
            row["last_updated"],
            row["published_date"],
            row["release_date"],
            row["short_description"],
            row["latest_release_version"],
            row["latest_release_asset_uri"],
            row["publisher_id"],
            row["extension_identifier"],
            row["install"],
            row["averagerating"],
            row["ratingcount"],
            row["trendingdaily"],
            row["trendingmonthly"],
            row["trendingweekly"],
            row["updateCount"],
            row["weightedRating"],
            row["downloadCount"],
        )
        for _, row in extensions_df.iterrows()
    ]

    for i in range(0, len(values), batch_size):
        batch = values[i : i + batch_size]
        upsert_data(logger, connection, "extensions", upsert_query, batch)
        logger.info(
            "upsert_extensions: Upserted extensions batch %d of %d rows",
            i // batch_size + 1,
            len(batch),
        )


def upsert_publishers(
    logger: Logger,
    connection: psycopg2.extensions.connection,
    publishers_df: pd.DataFrame,
    batch_size: int = 5000,
) -> None:
    """
    upsert_publishers upserts the given publishers to the database in batches
    """

    upsert_query = """
        INSERT INTO publishers (
            publisher_id, publisher_name, display_name, flags, domain, is_domain_verified
        ) VALUES %s
        ON CONFLICT (publisher_id) DO UPDATE SET
            publisher_name = EXCLUDED.publisher_name,
            display_name = EXCLUDED.display_name,
            flags = EXCLUDED.flags,
            domain = EXCLUDED.domain,
            is_domain_verified = EXCLUDED.is_domain_verified;
    """

    values = [
        (
            row["publisher_id"],
            row["publisher_name"],
            row["display_name"],
            row["flags"],
            row["domain"],
            row["is_domain_verified"],
        )
        for _, row in publishers_df.iterrows()
    ]

    for i in range(0, len(values), batch_size):
        batch = values[i : i + batch_size]
        upsert_data(logger, connection, "publishers", upsert_query, batch)
        logger.info(
            "upsert_publishers: Upserted publishers batch %d of %d rows",
            i // batch_size + 1,
            len(batch),
        )


def upsert_releases(
    logger: Logger,
    connection: psycopg2.extensions.connection,
    releases_df: pd.DataFrame,
    batch_size: int = 5000,
) -> None:
    """
    upser_releases upserts the given releases to the database in batches
    """

    upsert_query = """
        INSERT INTO releases (
            release_id, version, extension_id, flags, last_updated
        ) VALUES %s
        ON CONFLICT (release_id) DO UPDATE SET
            version = EXCLUDED.version,
            extension_id = EXCLUDED.extension_id,
            flags = EXCLUDED.flags,
            last_updated = EXCLUDED.last_updated;
    """

    values = [
        (
            row["release_id"],
            row["version"],
            row["extension_id"],
            row["flags"],
            row["last_updated"],
        )
        for _, row in releases_df.iterrows()
    ]

    for i in range(0, len(values), batch_size):
        batch = values[i : i + batch_size]
        upsert_data(logger, connection, "releases", upsert_query, values)
        logger.info(
            "upsert_releases: Upserted releases batch %d of %d rows",
            i // batch_size + 1,
            len(batch),
        )


def select_extensions(
    logger: Logger,
    connection: psycopg2.extensions.connection,
) -> pd.DataFrame:
    """
    select_extensions retrieves all extensions from the database in chunks
    """

    query = """
        SELECT
            extension_id,
            extension_name,
            display_name,
            flags,
            last_updated,
            published_date,
            release_date,
            short_description,
            latest_release_version,
            publisher_id,
            extension_identifier
        FROM
            extensions;
    """

    chunks = []
    for chunk in pd.read_sql_query(query, connection, chunksize=10000):
        chunks.append(chunk)
        logger.info(
            "select_extensions: Processed chunk of extensions with %d rows", len(chunk)
        )

    return pd.concat(chunks, ignore_index=True)


def select_publishers(
    logger: Logger,
    connection: psycopg2.extensions.connection,
) -> pd.DataFrame:
    """
    select_publishers retrieves all publishers from the database in chunks
    """

    query = """
        SELECT
            publisher_id,
            publisher_name,
            display_name,
            flags,
            domain,
            is_domain_verified
        FROM
            publishers;
    """

    chunks = []
    for chunk in pd.read_sql_query(query, connection, chunksize=10000):
        chunks.append(chunk)
        logger.info(
            "select_publishers: Processed chunk of publishers with %d rows", len(chunk)
        )

    return pd.concat(chunks, ignore_index=True)


def select_releases(
    logger: Logger,
    connection: psycopg2.extensions.connection,
) -> pd.DataFrame:
    """
    select_releases retrieves all releases from the database in chunks
    """

    query = """
        SELECT
            release_id,
            extension_id,
            version,
            last_updated,
            flags,
            uploaded_to_s3
        FROM
            releases;
    """

    chunks = []
    for chunk in pd.read_sql_query(query, connection, chunksize=10000):
        chunks.append(chunk)
        logger.info(
            "select_releases: Processed chunk of releases with %d rows", len(chunk)
        )

    return pd.concat(chunks, ignore_index=True)


def is_uploaded_to_s3(
    logger: Logger,
    connection: psycopg2.extensions.connection,
    extension_id: str,
    extension_version: str,
) -> bool:
    """
    is_uploaded_to_s3 retrieves the uploaded to S3 status for the given extension
    release from the releases table
    """

    query = f"""
        SELECT uploaded_to_s3
        FROM releases
        WHERE extension_id = '{extension_id}' AND version = '{extension_version}';
    """

    cursor = connection.cursor()
    cursor.execute(query)
    if cursor.rowcount > 0:
        result = cursor.fetchone()
        cursor.close()

        logger.info(
            "is_uploaded_to_s3: Fetched upload status for version %s of extension %s",
            extension_version,
            extension_id,
        )
        return result[0]

    cursor.close()

    logger.info(
        "is_uploaded_to_s3: No S3 upload status for version %s of extension %s was found",
        extension_version,
        extension_id,
    )
    return False


def get_old_latest_release_version(
    logger: Logger,
    connection: psycopg2.extensions.connection,
    extension_identifier: str,
) -> str:
    """
    get_old_latest_release_version retrieves the latest extension release from the
    extensions table
    """

    query = f"""
        SELECT latest_release_version
        FROM extensions
        WHERE extension_identifier = '{extension_identifier}';
    """

    cursor = connection.cursor()
    cursor.execute(query)

    if cursor.rowcount > 0:
        result = cursor.fetchone()
        cursor.close()

        logger.info(
            "get_old_latest_release_version: Fetched latest release version "
            "from the extensions table for extension %s",
            extension_identifier,
        )
        return result[0]

    cursor.close()

    logger.info(
        "get_old_latest_release_version: No latest release version from the extensions table "
        "for extension %s was found",
        extension_identifier,
    )
    return ""
