"""
vscavator.py orchestrates the extension retrieval process
"""

import time
import logging
import logging.config
from logging import Logger
import boto3
from botocore.client import BaseClient
import requests
from dateutil import parser
from packaging import version
import pandas as pd
import psycopg2
from dotenv import load_dotenv

from db import connect_to_database, create_all_tables, upsert_extensions, \
    upsert_publishers, upsert_releases, get_old_latest_release_version, \
    select_extensions, select_publishers, select_releases
from s3 import upload_all_extensions_to_s3, get_all_object_keys
from df import combine_dataframes, object_keys_to_dataframe, verified_uploaded_to_s3

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOGGER = logging.getLogger("vscavator")

EXTENSIONS_URL = "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery"
HEADERS = {
    "Content-Type": "application/json",
    "accept": "application/json;api-version=7.2-preview.1;excludeUrls=true",
}
REQUESTS_TIMEOUT = 10
EXTENSIONS_PAGE_SIZE = 2
EXTENSIONS_LAST_PAGE_NUMBER = 2
REQUESTS_SLEEP = 60

def get_extensions(
    logger: Logger,
    page_number: int,
    page_size: int
) -> list:
    """
    get_extensions fetches extension metadata from the VSCode Marketplace
    """

    payload = {
        'filters': [
            {
                'criteria': [
                    {
                        'filterType': 8,
                        'value': 'Microsoft.VisualStudio.Code',
                    },
                    {
                        'filterType': 10,
                        'value': 'target:"Microsoft.VisualStudio.Code" ',
                    },
                ],
                'pageSize': page_size,
                'pageNumber': page_number,
            },
        ],
    }

    response = requests.post(
        EXTENSIONS_URL, headers=HEADERS, json=payload, timeout=REQUESTS_TIMEOUT
    )

    if response.status_code == 200:
        results = response.json()["results"][0]["extensions"]
        logger.info(
            "Fetched extensions from page number %d with page size %d",
            page_number, page_size
        )
        return results

    logger.error(
        "Error fetching extensions from page number %d with page size %d: status code %d",
        page_number, page_size, response.status_code
    )
    return []

def get_extension_releases(
    logger: Logger,
    extension_identifier: str
) -> dict:
    """
    get_extension_releases fetches releases metadata for a given extension from
    the VSCode Marketplace
    """

    json_data = {
        'assetTypes': None,
        'filters': [
            {
                'criteria': [
                    {
                        'filterType': 7,
                        'value': extension_identifier,
                    },
                ],
                'pageSize': 100,
                'pageNumber': 1,
            },
        ],
        'flags': 2151,
    }

    response = requests.post(
        EXTENSIONS_URL, json=json_data, headers=HEADERS, timeout=REQUESTS_TIMEOUT
    )

    if response.status_code == 200:
        results = response.json()["results"][0]["extensions"][0]
        logger.info(
            "Fetched extension releases for extension %s",
            extension_identifier
        )
        return results
    elif response.status_code == 429:
        logging.warning(
            "Received 429 Too Many Requests error while fetching extension releases for %s... "
            "sleeping for %d seconds",
            extension_identifier, REQUESTS_SLEEP
        )
        time.sleep(REQUESTS_SLEEP)

    logger.error(
        "Error fetching extension releases for extension %s: %d",
        extension_identifier, response.status_code
    )
    return {}

def get_all_extensions(
    logger: Logger,
    page_size: int = EXTENSIONS_PAGE_SIZE,
    last_page_number: int = EXTENSIONS_LAST_PAGE_NUMBER
) -> list:
    """
    get_all_extensions fetches all extension metadata from the VSCode Marketplace
    """

    all_extensions = []
    for page_number in range(1, last_page_number + 1):
        extensions = get_extensions(logger, page_number, page_size)
        all_extensions.extend(extensions)
    return all_extensions

def get_all_releases(
    logger: Logger,
    connection: psycopg2.extensions.connection,
    extensions_df: pd.DataFrame
) -> list:
    """
    get_all_releases fetches all release metadata from the VSCode Marketplace
    """

    all_releases = []
    extension_identifiers = extensions_df["extension_identifier"].tolist()

    for extension_identifier in extension_identifiers:
        # Find the latest release version in the database from the previous run
        old_latest_release_version = get_old_latest_release_version(
            logger, connection, extension_identifier
        )

        # Find the latest release version from the newly collected extension data
        new_latest_release_version = get_new_latest_release_version(
            extensions_df, extension_identifier
        )

        # Check if the latest release has already been fetched for the extension in a previous run
        if old_latest_release_version == new_latest_release_version:
            logger.info(
                "Skipped fetching the releases for %s since they have already been retrieved",
                extension_identifier
            )
            continue

        releases = get_extension_releases(logger, extension_identifier)
        all_releases.append(releases)

    return all_releases

def extract_publisher_metadata(
    extensions: list
) -> pd.DataFrame:
    """
    extract_publisher_metadata extracts relevant publisher information from the raw data
    """

    publishers_metadata = []
    unique_publishers = set()

    for extension in extensions:
        publisher_metadata = extension["publisher"]
        publisher_id = publisher_metadata["publisherId"]

        # Deduplicate publisher data
        if publisher_id in unique_publishers:
            continue
        unique_publishers.add(publisher_id)

        publishers_metadata.append({
            "publisher_id": publisher_id,
            "publisher_name": publisher_metadata["publisherName"],
            "display_name": publisher_metadata["displayName"],
            "flags": publisher_metadata["flags"].split(", "),
            "domain": publisher_metadata["domain"],
            "is_domain_verified": publisher_metadata["isDomainVerified"]
        })

    return pd.DataFrame(publishers_metadata)

def extract_extension_metadata(
    extensions: list
) -> pd.DataFrame:
    """
    extract_extension_metadata extracts relevant extension information from the raw data
    """

    extensions_metadata = []

    for extension in extensions:
        extension_name = extension["extensionName"]
        publisher_name = extension["publisher"]["publisherName"]
        extension_identifier = publisher_name + "." + extension_name

        extensions_metadata.append({
            "extension_id": extension["extensionId"],
            "extension_name": extension["extensionName"],
            "display_name": extension["displayName"],
            "flags": extension["flags"].split(", "),
            "last_updated": parser.isoparse(extension["lastUpdated"]),
            "published_date": parser.isoparse(extension["publishedDate"]),
            "release_date": parser.isoparse(extension["releaseDate"]),
            "short_description": extension["shortDescription"],
            "latest_release_version": get_latest_version(extension["versions"]),
            "publisher_id": extension["publisher"]["publisherId"],
            "extension_identifier": extension_identifier
        })

    return pd.DataFrame(extensions_metadata)

def extract_release_metadata(
    releases: list
) -> pd.DataFrame:
    """
    extract_release_metadata extracts the relevant release information from the raw data
    """

    extension_releases = []
    release_ids = set()

    for extension in releases:
        extension_id = extension["extensionId"]
        extension_versions = extension["versions"]

        for rextension_release in extension_versions:
            extension_version = rextension_release["version"]
            release_id = extension_id + "-" + extension_version

            # Deduplicate release data
            # TODO: This shouldn't be necessary but the release ID is not always unique
            if release_id in release_ids:
                continue
            release_ids.add(release_id)

            extension_releases.append({
                "release_id": release_id,
                "version": extension_version,
                "flags": rextension_release["flags"].split(", "),
                "last_updated": parser.isoparse(rextension_release["lastUpdated"]),
                "extension_id": extension_id,
            })

    return pd.DataFrame(
        extension_releases,
        columns=["release_id", "version", "extension_id", "flags", "last_updated"]
    )

def get_latest_version(
    versions: list
) -> str:
    """
    get_latest_version finds the most up-to-date version from a list of extension releases
    """

    return max(versions, key=lambda x: version.parse(x["version"]))["version"]

def get_new_latest_release_version(
    extensions_df: pd.DataFrame,
    extension_identifier: str
) -> str:
    """
    get_new_latest_release_version finds the latest release version for the given extension
    """

    latest_release_version = extensions_df.loc[
        extensions_df["extension_identifier"] == extension_identifier, "latest_release_version"
    ]

    if not latest_release_version.empty:
        return latest_release_version.iloc[-1]

    return ""

def validate_data_consistency(
    logger: Logger,
    connection: psycopg2.extensions.connection,
    s3_client: BaseClient
) -> None:
    """
    validate_data_consistency checks that the data in the database matches what exists in S3
    """

    # Get the names of all objects stored in S3
    object_keys = get_all_object_keys(s3_client)
    object_keys_df = object_keys_to_dataframe(object_keys)

    # Get all extension, publisher, and release data from the database
    extensions_df = select_extensions(logger, connection)
    publishers_df = select_publishers(logger, connection)
    releases_df = select_releases(logger, connection)

    combined_df = combine_dataframes(extensions_df, publishers_df, releases_df)

    for _, row in combined_df.iterrows():
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
                "Publisher %s, extension %s, version %s: "
                "database state uploaded_to_s3: %s, while S3 state uploaded_to_s3: %s",
                publisher_name, extension_name, extension_version, db_uploaded_to_s3,
                s3_uploaded_to_s3
            )

def main() -> None:
    """
    main handles the entire extension retrieval process
    """

    # Setup the database
    connection = connect_to_database(LOGGER)
    create_all_tables(LOGGER, connection)

    # Retrieve extension, publisher, and release data
    extensions = get_all_extensions(LOGGER)
    extensions_df = extract_extension_metadata(extensions)
    publishers_df = extract_publisher_metadata(extensions)
    releases = get_all_releases(LOGGER, connection, extensions_df)
    releases_df = extract_release_metadata(releases)

    # Insert extension, publisher, and release data into the database
    upsert_publishers(LOGGER, connection, publishers_df)
    upsert_extensions(LOGGER, connection, extensions_df)
    upsert_releases(LOGGER, connection, releases_df)

    # Upload extension files to S3
    s3_client = boto3.client("s3")
    combined_df = combine_dataframes(extensions_df, publishers_df, releases_df)
    upload_all_extensions_to_s3(LOGGER, connection, s3_client, combined_df)

    # Check the consistency of data in the database and S3
    validate_data_consistency(LOGGER, connection, s3_client)

    # Close all connections
    s3_client.close()
    connection.close()

if __name__ == "__main__":
    main()
