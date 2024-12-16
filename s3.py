"""
TODO
"""

import os
from logging import Logger
from botocore.client import BaseClient
import requests
from packaging import version
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
    connection: psycopg2.extensions.connection,
    s3_client: BaseClient,
    extension_id: str,
    publisher_name: str,
    extension_name: str,
    extension_version: str
) -> None:
    """
    TODO
    """

    url = DOWNLOAD_URL.format(
        publisher=publisher_name, name=extension_name, version=extension_version
    )

    try:
        response = requests.get(url, stream=True, timeout=REQUESTS_TIMEOUT)
        s3_key = f"extensions/{publisher_name}/{extension_name}/{extension_version}.vsix"
        try:
            s3_client.upload_fileobj(response.raw, os.getenv("S3_BUCKET_NAME"), s3_key)
            logger.info(
                "Uploaded extension to S3: s3://%s/%s",
                os.getenv("S3_BUCKET_NAME"), s3_key
            )

            update_query = f"""
                UPDATE releases
                SET uploaded_to_s3 = TRUE
                WHERE extension_id = '{extension_id}' AND version = '{extension_version}';
            """
            cursor = connection.cursor()
            cursor.execute(update_query)
            if cursor.rowcount > 0:
                connection.commit()
                logger.info(
                    "Updated uploaded_to_s3 status to TRUE for version %s of extension %s",
                    extension_version, extension_name
                )
            else:
                logger.error(
                    "Failed to update uploaded_to_s3 status to True for version %s of extension %s",
                    extension_version, extension_name
                )

            cursor.close()
        except Exception as e: # pylint: disable=broad-exception-caught
            logger.error(
                "Error uploading extension %s version %s by publisher %s to S3: %s",
                extension_name, extension_version, publisher_name, e
            )
    except Exception as e: # pylint: disable=broad-exception-caught
        logger.error(
            "Error downloading extension %s version %s by publisher %s from marketplace: %s",
            extension_name, version, publisher_name, response.status_code, e
        )

def upload_all_extensions_to_s3(
    logger: Logger,
    connection: psycopg2.extensions.connection,
    s3_client: BaseClient,
    combined_df: pd.DataFrame
) -> None:
    """
    TODO
    """

    unique_extensions = set()
    for _, row in combined_df.iterrows():
        extension_id = row["extension_id"]
        if extension_id in unique_extensions:
            continue
        unique_extensions.add(extension_id)

        publisher_name = row["publisher_name"]
        extension_name = row["extension_name"]
        extension_version = row["version"]

        if is_uploaded_to_s3(logger, connection, extension_id, extension_version):
            logger.info(
                "Skipped uploading version %s of extension %s to S3 since it has already been "
                "uploaded",
                extension_version, extension_name
            )
            continue

        upload_extension_to_s3(
            logger, connection, s3_client, extension_id, publisher_name,
            extension_name, extension_version
        )

def get_all_object_keys(
    s3_client: BaseClient
) -> list:
    """
    TODO
    """

    paginator = s3_client.get_paginator('list_objects_v2')
    return [
        s3_object["Key"]
        for page in paginator.paginate(Bucket=os.getenv("S3_BUCKET_NAME"))
        for s3_object in page["Contents"]
    ]
