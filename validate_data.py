"""Checks the consistency of the data in the database and S3"""

import os
from logging import Logger
from botocore.client import BaseClient
import boto3
import pandas as pd
import psycopg2

from util import (
    connect_to_database,
    select_data,
    combine_dataframes,
    select_extensions,
    select_publishers,
)


def get_all_object_keys(s3_client: BaseClient) -> list:
    """Retrieves all object key names from the bucket"""

    paginator = s3_client.get_paginator("list_objects_v2")
    return [
        s3_object["Key"]
        for page in paginator.paginate(Bucket=os.getenv("S3_BUCKET_NAME"))
        for s3_object in page["Contents"]
    ]


def object_keys_to_dataframe(object_keys: list) -> pd.DataFrame:
    """Extracts the publisher name, extension name, and extension version from the S3 object keys"""

    parsed_object_keys = []

    for object_key in object_keys:
        fields = object_key.split("/")
        parsed_object_keys.append(
            {
                "publisher_name": fields[1],
                "extension_name": fields[2],
                "version": fields[3].replace(".vsix", ""),
            }
        )

    return pd.DataFrame(parsed_object_keys)


def verified_uploaded_to_s3(
    object_keys_df: pd.DataFrame, publisher_name: str, extension_name: str, version: str
) -> bool:
    """Checks if the status of given extension release in the dataframe is uploaded to S3"""

    return not object_keys_df.loc[
        (object_keys_df["publisher_name"] == publisher_name)
        & (object_keys_df["extension_name"] == extension_name)
        & (object_keys_df["version"] == version)
    ].empty


def select_releases(
    logger: Logger,
    connection: psycopg2.extensions.connection,
) -> pd.DataFrame:
    """Retrieves all releases from the database in chunks"""

    query = """
        SELECT
            extension_id,
            version,
            uploaded_to_s3
        FROM
            releases;
    """
    return select_data(logger, connection, "releases", query)


def validate_data(
    logger: Logger,
) -> None:
    """Checks that the data in the database matches what exists in S3"""

    # Setup
    connection = connect_to_database(logger)
    s3_client = boto3.client("s3")

    # Get the names of all objects stored in S3
    object_keys = get_all_object_keys(s3_client)
    object_keys_df = object_keys_to_dataframe(object_keys)

    # Get all extension, publisher, and release data from the database
    extensions_df = select_extensions(logger, connection)
    publishers_df = select_publishers(logger, connection)
    releases_df = select_releases(logger, connection)

    extensions_publishers_releases_df = combine_dataframes(
        [releases_df, extensions_df, publishers_df], ["extension_id", "publisher_id"]
    )

    for _, row in extensions_publishers_releases_df.iterrows():
        publisher_name = row["publisher_name"]
        extension_name = row["extension_name"]
        extension_version = row["version"]

        db_uploaded_to_s3 = row["uploaded_to_s3"]
        s3_uploaded_to_s3 = verified_uploaded_to_s3(
            object_keys_df, publisher_name, extension_name, extension_version
        )

        # Check if what exists in S3 matches the database state
        if db_uploaded_to_s3 != s3_uploaded_to_s3:
            logger.error(
                "validate_data_consistency: Publisher %s, extension %s, version %s: "
                "database state uploaded_to_s3: %s, while S3 state uploaded_to_s3: %s",
                publisher_name,
                extension_name,
                extension_version,
                db_uploaded_to_s3,
                s3_uploaded_to_s3,
            )

    # Close
    connection.close()
    s3_client.close()
