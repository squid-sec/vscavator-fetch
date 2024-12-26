"""
TODO
"""

import os
from typing import List
from logging import Logger
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import responses


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


def select_data(
    logger: Logger,
    connection: psycopg2.extensions.connection,
    table_name: str,
    select_data_query: str,
) -> pd.DataFrame:
    """
    select_data executes the select data query on the given table
    """

    chunks = []
    for chunk in pd.read_sql_query(select_data_query, connection, chunksize=10000):
        chunks.append(chunk)
        logger.info(
            "select_data: Processed chunk of %s with %d rows", table_name, len(chunk)
        )

    return pd.concat(chunks, ignore_index=True)


def clean_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    clean_dataframe prepares the given data from to be upserted to the database
    """

    # Handling missing values in datetime columns by replacing NaT with None
    for col in dataframe.select_dtypes(include=["datetime64[ns]"]):
        dataframe[col] = dataframe[col].apply(lambda x: None if pd.isna(x) else x)

    # Remove duplicate rows
    dataframe = dataframe.drop_duplicates()

    # Strip leading and trailing spaces from string columns
    for col in dataframe.select_dtypes(include=["object"]):
        dataframe[col] = dataframe[col].str.strip()

    return dataframe


def combine_dataframes(
    dataframes: List[pd.DataFrame], keys: List[str], how: str = "inner"
) -> pd.DataFrame:
    """
    combine_dataframes is a generic function to merge multiple dataframes based on specified keys
    """

    if len(dataframes) - 1 != len(keys):
        raise ValueError(
            "Number of keys must be one less than the number of dataframes."
        )

    combined_df = dataframes[0]
    for i, key in enumerate(keys):
        combined_df = combined_df.merge(dataframes[i + 1], on=key, how=how)

    return combined_df


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
            extension_identifier,
            extension_name,
            publisher_id,
            latest_release_version
        FROM
            extensions;
    """
    return select_data(logger, connection, "extensions", query)


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
            publisher_name
        FROM
            publishers;
    """
    return select_data(logger, connection, "publishers", query)


def select_latest_releases(
    logger: Logger,
    connection: psycopg2.extensions.connection,
) -> pd.DataFrame:
    """
    select_releases retrieves the latest releases for each extension from the database in chunks
    """

    query = """
        WITH ranked_releases AS (
            SELECT
                extension_id,
                version,
                uploaded_to_s3,
                ROW_NUMBER() OVER (
                    PARTITION BY extension_id 
                    ORDER BY
                        string_to_array(version, '.')::int[] DESC
                ) AS row_num
            FROM releases
        )
        SELECT
            extension_id,
            version,
            uploaded_to_s3
        FROM
            ranked_releases
        WHERE
            row_num = 1;
    """
    return select_data(logger, connection, "releases", query)


def add_mock_response(url, mock_response, status=200):
    """
    Utility to add a mocked API response to the responses library.
    """
    responses.add(
        responses.POST,
        url,
        json=mock_response,
        status=status,
    )
