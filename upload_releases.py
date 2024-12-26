"""Downloads .vsix extension files from the VSCode marketplace and uploads them to S3"""

import os
from logging import Logger
import boto3
from botocore.client import BaseClient
import requests
import psycopg2
import pandas as pd

from util import (
    connect_to_database,
    combine_dataframes,
    select_extensions,
    select_publishers,
    select_latest_releases,
)


def upload_extension_to_s3(
    logger: Logger,
    connection: psycopg2.extensions.connection,
    s3_client: BaseClient,
    extension_info: dict,
) -> bool:
    """Fetches the given extension from VSCode Marketplace and uploads the .vsix file to S3"""

    publisher_name = extension_info["publisher_name"]
    extension_name = extension_info["extension_name"]
    extension_version = extension_info["extension_version"]
    extension_id = extension_info["extension_id"]

    response = requests.get(
        f"https://marketplace.visualstudio.com/_apis/public/gallery/publishers/"
        f"{publisher_name}/vsextensions/{extension_name}/{extension_version}/vspackage",
        stream=True,
        timeout=5,
    )

    if response.status_code == 200:
        s3_key = (
            f"extensions/{publisher_name}/{extension_name}/{extension_version}.vsix"
        )

        s3_client.upload_fileobj(response.raw, os.getenv("S3_BUCKET_NAME"), s3_key)
        logger.info(
            "upload_extension_to_s3: Uploaded extension to S3: s3://%s/%s",
            os.getenv("S3_BUCKET_NAME"),
            s3_key,
        )

        update_query = f"""
            UPDATE releases
            SET uploaded_to_s3 = TRUE
            WHERE extension_id = '{extension_id}' AND version = '{extension_version}';
        """
        cursor = connection.cursor()
        cursor.execute(update_query)
        connection.commit()
        cursor.close()

        return True

    logger.error(
        "upload_extension_to_s3: Error downloading extension %s version %s by publisher %s "
        "from marketplace: status code %d",
        extension_name,
        extension_version,
        publisher_name,
        response.status_code,
    )
    return False


def upload_all_extensions_to_s3(
    logger: Logger,
    connection: psycopg2.extensions.connection,
    s3_client: BaseClient,
    combined_df: pd.DataFrame,
) -> None:
    """Fetches and uploads the given extensions to S3"""

    for _, row in combined_df.iterrows():
        extension_id = row["extension_id"]
        extension_name = row["extension_name"]
        publisher_name = row["publisher_name"]
        extension_version = row["version"]
        is_uploaded_to_s3 = row["uploaded_to_s3"]

        # Check if extension version has already been uploaded to S3
        if is_uploaded_to_s3:
            logger.info(
                "upload_all_extensions_to_s3: Skipped uploading version %s of extension %s "
                "to S3 since it has already been uploaded",
                extension_version,
                extension_name,
            )
            continue

        extension_info = {
            "extension_id": extension_id,
            "publisher_name": publisher_name,
            "extension_name": extension_name,
            "extension_version": extension_version,
        }

        success = upload_extension_to_s3(logger, connection, s3_client, extension_info)

        if not success:
            logger.error(
                "upload_all_extensions_to_s3: Failed to upload extension %s version %s by %s "
                "to S3",
                extension_name,
                extension_version,
                publisher_name,
            )


def upload_releases(logger: Logger):
    """Orchestrates the retrieval of extension files and their upload to S3"""

    # Setup
    connection = connect_to_database(logger)
    s3_client = boto3.client("s3")

    # Fetch the existing data from the database
    extensions_df = select_extensions(logger, connection)
    publishers_df = select_publishers(logger, connection)
    releases_df = select_latest_releases(logger, connection)
    extensions_publishers_releases_df = combine_dataframes(
        [releases_df, extensions_df, publishers_df], ["extension_id", "publisher_id"]
    )

    # Fetch the extensions from the marketplace and upload them to S3
    upload_all_extensions_to_s3(
        logger, connection, s3_client, extensions_publishers_releases_df
    )

    # Close
    connection.close()
    s3_client.close()
