"""
s3.py contains S3 related functions
"""

import os
from logging import Logger
from botocore.client import BaseClient
import requests
import psycopg2
import pandas as pd

from db import is_uploaded_to_s3

DOWNLOAD_URL = (
    "https://marketplace.visualstudio.com/_apis/public/gallery/"
    "publishers/{publisher}/vsextensions/{name}/{version}/vspackage"
)
REQUESTS_TIMEOUT = 10

def upload_extension_to_s3(
    logger: Logger,
    session: requests.Session,
    connection: psycopg2.extensions.connection,
    s3_client: BaseClient,
    extension_info: dict
) -> bool:
    """
    upload_extension_to_s3 fetches the given extension from VSCode Marketplace and
    uploads the .vsix file to S3
    """

    publisher_name = extension_info["publisher_name"]
    extension_name = extension_info["extension_name"]
    extension_version = extension_info["extension_version"]
    extension_id = extension_info["extension_id"]

    url = DOWNLOAD_URL.format(
        publisher=publisher_name, name=extension_name, version=extension_version
    )

    response = session.get(url, stream=True, timeout=REQUESTS_TIMEOUT)

    if response.status_code == 200:
        s3_key = f"extensions/{publisher_name}/{extension_name}/{extension_version}.vsix"

        s3_client.upload_fileobj(response.raw, os.getenv("S3_BUCKET_NAME"), s3_key)
        logger.info(
            "upload_extension_to_s3: Uploaded extension to S3: s3://%s/%s",
            os.getenv("S3_BUCKET_NAME"), s3_key
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
        extension_name, extension_version, publisher_name, response.status_code
    )
    return False

def upload_all_extensions_to_s3(
    logger: Logger,
    session: requests.Session,
    connection: psycopg2.extensions.connection,
    s3_client: BaseClient,
    combined_df: pd.DataFrame
) -> None:
    """
    upload_all_extensions_to_s3 fetches and uploads the given extensions to S3
    """

    unique_extensions = set()
    failed_extensions = []

    for _, row in combined_df.iterrows():
        # Only fetch the latest release of an extension
        extension_id = row["extension_id"]
        if extension_id in unique_extensions:
            continue
        unique_extensions.add(extension_id)

        publisher_name = row["publisher_name"]
        extension_name = row["extension_name"]
        extension_version = row["version"]

        # Check if extension version has already been uploaded to S3
        if is_uploaded_to_s3(logger, connection, extension_id, extension_version):
            logger.info(
                "upload_all_extensions_to_s3: Skipped uploading version %s of extension %s "
                "to S3 since it has already been uploaded",
                extension_version, extension_name
            )
            continue

        extension_info = {
            "extension_id": extension_id,
            "publisher_name": publisher_name,
            "extension_name": extension_name,
            "extension_version": extension_version
        }

        success = upload_extension_to_s3(
            logger, session, connection, s3_client, extension_info
        )

        if not success:
            failed_extensions.append(
                (extension_id, publisher_name, extension_name, extension_version)
            )

    if len(failed_extensions) > 0:
        logger.warning(
            "upload_all_extensions_to_s3: Failed to upload %d extensions to S3... trying again",
            len(failed_extensions)
        )

        for extension_id, publisher_name, extension_name, extension_version in failed_extensions:
            extension_info = {
                "extension_id": extension_id,
                "publisher_name": publisher_name,
                "extension_name": extension_name,
                "extension_version": extension_version
            }

            success = upload_extension_to_s3(
                logger, session, connection, s3_client, extension_info
            )

            if not success:
                logger.error(
                    "upload_all_extensions_to_s3: Failed to upload extension %s version %s by %s "
                    "to S3 for a second time",
                    extension_name, extension_version, publisher_name
                )

def get_all_object_keys(
    s3_client: BaseClient
) -> list:
    """
    get_all_object_keys retrieves all object key names from the bucket
    """

    paginator = s3_client.get_paginator('list_objects_v2')
    return [
        s3_object["Key"]
        for page in paginator.paginate(Bucket=os.getenv("S3_BUCKET_NAME"))
        for s3_object in page["Contents"]
    ]
