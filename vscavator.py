"""
vscavator.py orchestrates the extension retrieval process
"""

import os
import time
import logging
import logging.config
from logging import Logger
import boto3
from botocore.client import BaseClient
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from dateutil import parser
import pandas as pd
import psycopg2
from dotenv import load_dotenv

from db import (
    connect_to_database,
    create_all_tables,
    upsert_extensions,
    upsert_publishers,
    upsert_releases,
    get_old_latest_release_version,
    select_extensions,
    select_publishers,
    select_releases,
)
from s3 import upload_all_extensions_to_s3, get_all_object_keys
from df import combine_dataframes, object_keys_to_dataframe, verified_uploaded_to_s3

EXTENSIONS_URL = (
    "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery"
)
HEADERS = {"accept": "application/json;api-version=7.2-preview.1;"}

EXTENSIONS_PAGE_SIZE = 100
RELEASES_PAGE_SIZE = 100

REQUESTS_TIMEOUT = 10
REQUESTS_DELAY = 0.25

DEFAULT_DATE = "1970-01-01T00:00:00"

SORT_BY = 2  # Title
SORT_ORDER = 1  # Ascending
EXTENSIONS_FLAGS = 0x100  # Include statistics
RELEASES_FLAGS = 0x1  # Include versions


def get_total_number_of_extensions() -> int:
    """
    get_total_number_of_extensions finds the total number of extensions in the marketplace
    """

    payload = {
        "filters": [
            {
                "criteria": [
                    {
                        "filterType": 8,
                        "value": "Microsoft.VisualStudio.Code",
                    },
                    {
                        "filterType": 10,
                        "value": 'target:"Microsoft.VisualStudio.Code"',
                    },
                ],
                "pageSize": 1,
                "pageNumber": 1,
            },
        ],
        "flags": EXTENSIONS_FLAGS,
    }

    response = requests.post(EXTENSIONS_URL, headers=HEADERS, json=payload, timeout=5)
    result_metadata = response.json()["results"][0]["resultMetadata"]

    total_count = None
    for metadata in result_metadata:
        if metadata["metadataType"] == "ResultCount":
            for item in metadata["metadataItems"]:
                if item["name"] == "TotalCount":
                    total_count = item["count"]
                    break

    return total_count


def calculate_number_of_extension_pages(num_extensions: int) -> int:
    """
    calculate_number_of_extension_pages calculates the number of extension pages to fetch
    """

    return num_extensions // EXTENSIONS_PAGE_SIZE + 1


def get_extensions(logger: Logger, session: requests.Session, page_number: int) -> list:
    """
    get_extensions fetches extension metadata from the VSCode Marketplace
    """

    payload = {
        "filters": [
            {
                "criteria": [
                    {
                        "filterType": 8,
                        "value": "Microsoft.VisualStudio.Code",
                    },
                    {
                        "filterType": 10,
                        "value": 'target:"Microsoft.VisualStudio.Code"',
                    },
                ],
                "pageSize": EXTENSIONS_PAGE_SIZE,
                "pageNumber": page_number,
                "sortBy": SORT_BY,
                "sortOrder": SORT_ORDER,
            },
        ],
        "flags": EXTENSIONS_FLAGS,
    }

    response = session.post(
        EXTENSIONS_URL, headers=HEADERS, json=payload, timeout=REQUESTS_TIMEOUT
    )

    if response.status_code == 200:
        extensions = response.json()["results"][0]["extensions"]
        logger.info(
            "get_extensions: Fetched %d extensions from page number %d",
            len(extensions),
            page_number,
        )
        return extensions

    logger.critical(
        "get_extensions: Error fetching extensions from page number %d: "
        "status code %d",
        page_number,
        response.status_code,
    )
    return []


def get_extension_releases(
    logger: Logger,
    session: requests.Session,
    extension_identifier: str,
    page_number: int = 1,
) -> dict:
    """
    get_extension_releases fetches releases metadata for a given extension from
    the VSCode Marketplace
    """

    json_data = {
        "assetTypes": None,
        "filters": [
            {
                "criteria": [
                    {
                        "filterType": 7,
                        "value": extension_identifier,
                    },
                ],
                "pageSize": RELEASES_PAGE_SIZE,
                "pageNumber": page_number,
            },
        ],
        "flags": RELEASES_FLAGS,
    }

    response = session.post(
        EXTENSIONS_URL, json=json_data, headers=HEADERS, timeout=REQUESTS_TIMEOUT
    )

    if response.status_code == 200:
        releases = response.json()["results"][0]["extensions"][0]["versions"]
        logger.info(
            "get_extension_releases: Fetched extension releases for extension %s",
            extension_identifier,
        )
        return releases

    logger.error(
        "get_extension_releases: Error fetching release metadata for extension %s "
        "from page number %d: status code %d",
        extension_identifier,
        page_number,
        response.status_code,
    )
    return {}


def get_all_extensions(
    logger: Logger,
    session: requests.Session,
    last_page_number: int,
) -> list:
    """
    get_all_extensions fetches all extension metadata from the VSCode Marketplace
    """

    all_extensions = []

    for page_number in range(1, last_page_number + 1):
        time.sleep(REQUESTS_DELAY)
        extensions = get_extensions(logger, session, page_number)
        all_extensions.extend(extensions)

    return all_extensions


def get_all_releases(
    logger: Logger,
    session: requests.Session,
    connection: psycopg2.extensions.connection,
    extensions_df: pd.DataFrame,
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
            logger, extensions_df, extension_identifier
        )

        # Check if the latest release has already been fetched for the extension in a previous run
        if old_latest_release_version == new_latest_release_version:
            logger.info(
                "get_all_releases: Skipped fetching the releases for %s "
                "since they have already been retrieved",
                extension_identifier,
            )
            continue

        time.sleep(REQUESTS_DELAY)

        releases = get_extension_releases(logger, session, extension_identifier)
        all_releases.append(releases)

    return all_releases


def extract_publisher_metadata(logger: Logger, extensions: list) -> pd.DataFrame:
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
            logger.info(
                "extract_publisher_metadata: Duplicate publisher found with ID %s",
                publisher_id,
            )
            continue
        unique_publishers.add(publisher_id)

        publishers_metadata.append(
            {
                "publisher_id": publisher_id,
                "publisher_name": publisher_metadata["publisherName"],
                "display_name": publisher_metadata["displayName"],
                "flags": publisher_metadata["flags"].split(", "),
                "domain": publisher_metadata["domain"],
                "is_domain_verified": publisher_metadata["isDomainVerified"],
            }
        )

    return pd.DataFrame(publishers_metadata)


def extract_extension_metadata(extensions: list) -> pd.DataFrame:
    """
    extract_extension_metadata extracts relevant extension information from the raw data
    """

    unique_extensions = set()
    extensions_metadata = []

    for extension in extensions:
        extension_id = extension["extensionId"]
        if extension_id in unique_extensions:
            continue
        unique_extensions.add(extension_id)

        extension_name = extension["extensionName"]
        publisher_name = extension["publisher"]["publisherName"]
        extension_identifier = publisher_name + "." + extension_name

        latest_version = extension["versions"][0]
        properties = latest_version.get("properties", {})
        github_url = extract_github_url(properties)
        statistics = extract_statistics(extension["statistics"])

        extensions_metadata.append(
            {
                "extension_id": extension_id,
                "extension_name": extension["extensionName"],
                "display_name": extension["displayName"],
                "flags": extension["flags"].split(", "),
                "last_updated": parser.isoparse(extension["lastUpdated"]),
                "published_date": parser.isoparse(extension["publishedDate"]),
                "release_date": parser.isoparse(extension["releaseDate"]),
                "short_description": extension.get("shortDescription", ""),
                "latest_release_version": latest_version["version"],
                "latest_release_asset_uri": latest_version["assetUri"],
                "publisher_id": extension["publisher"]["publisherId"],
                "extension_identifier": extension_identifier,
                "github_url": github_url,
                "install": statistics["install"],
                "averagerating": statistics["averagerating"],
                "ratingcount": statistics["ratingcount"],
                "trendingdaily": statistics["trendingdaily"],
                "trendingmonthly": statistics["trendingmonthly"],
                "trendingweekly": statistics["trendingweekly"],
                "updateCount": statistics["updateCount"],
                "weightedRating": statistics["weightedRating"],
                "downloadCount": statistics["downloadCount"],
            }
        )

    return pd.DataFrame(extensions_metadata)


def extract_release_metadata(logger: Logger, releases: list) -> pd.DataFrame:
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
            # This shouldn't be necessary but the release ID is not always unique
            if release_id in release_ids:
                logger.info(
                    "extract_release_metadata: Duplicate extension release found "
                    "with release ID %s",
                    release_id,
                )
                continue
            release_ids.add(release_id)

            extension_releases.append(
                {
                    "release_id": release_id,
                    "version": extension_version,
                    "flags": rextension_release["flags"].split(", "),
                    "last_updated": parser.isoparse(rextension_release["lastUpdated"]),
                    "extension_id": extension_id,
                }
            )

    return pd.DataFrame(
        extension_releases,
        columns=["release_id", "version", "extension_id", "flags", "last_updated"],
    )


def extract_statistics(statistics: list) -> dict:
    """
    extract_statistics finds the extension statistics
    """

    extension_stats = {
        "install": 0,
        "averagerating": 0,
        "ratingcount": 0,
        "trendingdaily": 0,
        "trendingmonthly": 0,
        "trendingweekly": 0,
        "updateCount": 0,
        "weightedRating": 0,
        "downloadCount": 0,
    }

    for stat in statistics:
        name = stat["statisticName"]
        value = stat["value"]
        if name in extension_stats:
            extension_stats[name] = value

    return extension_stats


def extract_github_url(properties: list) -> str:
    """
    extract_github_url finds the GitHub URL of the extension
    """

    github_url = ""
    for extension_property in properties:
        if extension_property["key"] == "Microsoft.VisualStudio.Services.Links.GitHub":
            github_url = extension_property["value"]
            break
    return github_url


def get_new_latest_release_version(
    logger: Logger, extensions_df: pd.DataFrame, extension_identifier: str
) -> str:
    """
    get_new_latest_release_version finds the latest release version for the given extension
    """

    latest_release_version = extensions_df.loc[
        extensions_df["extension_identifier"] == extension_identifier,
        "latest_release_version",
    ]

    if latest_release_version.empty:
        logger.info(
            "get_new_latest_release_version: Failed to get new latest release version from %s",
            extension_identifier,
        )
        return ""

    return latest_release_version.iloc[-1]


def validate_data_consistency(
    logger: Logger, connection: psycopg2.extensions.connection, s3_client: BaseClient
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
                "validate_data_consistency: Publisher %s, extension %s, version %s: "
                "database state uploaded_to_s3: %s, while S3 state uploaded_to_s3: %s",
                publisher_name,
                extension_name,
                extension_version,
                db_uploaded_to_s3,
                s3_uploaded_to_s3,
            )


def configure_requests_session() -> requests.Session:
    """
    configure_requests_session creates requests session with configurable retry strategy
    """

    retry_strategy = Retry(
        total=5,
        backoff_factor=4,
        status_forcelist=[429],
        allowed_methods=["GET", "POST"],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)

    return session


def configure_logger() -> Logger:
    """
    configure_logger configures the logger for the application
    """

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logger = logging.getLogger(os.getenv("LOGGER_NAME"))
    return logger


def log_constants(logger: Logger) -> None:
    """
    log_constants logs the constants used in the script
    """

    logger.info("log_constants: EXTENSIONS_URL: %s", EXTENSIONS_URL)
    logger.info("log_constants: HEADERS: %s", HEADERS)
    logger.info("log_constants: EXTENSIONS_PAGE_SIZE: %s", EXTENSIONS_PAGE_SIZE)
    logger.info("log_constants: RELEASES_PAGE_SIZE: %s", RELEASES_PAGE_SIZE)
    logger.info("log_constants: REQUESTS_TIMEOUT: %s", REQUESTS_TIMEOUT)
    logger.info("log_constants: REQUESTS_DELAY: %s", REQUESTS_DELAY)
    logger.info("log_constants: DEFAULT_DATE: %s", DEFAULT_DATE)
    logger.info("log_constants: SORT_BY: %s", SORT_BY)
    logger.info("log_constants: SORT_ORDER: %s", SORT_ORDER)
    logger.info("log_constants: EXTENSIONS_FLAGS: %s", EXTENSIONS_FLAGS)
    logger.info("log_constants: RELEASES_FLAGS: %s", RELEASES_FLAGS)


def main() -> None:
    """
    main handles the entire extension retrieval process
    """

    # Setup script
    load_dotenv()
    logger = configure_logger()
    log_constants(logger)
    session = configure_requests_session()

    # Setup the database
    connection = connect_to_database(logger)
    create_all_tables(logger, connection)

    # Find number of pages of extensions to fetch
    num_total_extensions = get_total_number_of_extensions()
    num_extension_pages = calculate_number_of_extension_pages(num_total_extensions)

    # Retrieve extension, publisher, and release data
    extensions = get_all_extensions(logger, session, num_extension_pages)
    extensions_df = extract_extension_metadata(extensions)
    publishers_df = extract_publisher_metadata(logger, extensions)
    releases = get_all_releases(logger, session, connection, extensions_df)
    releases_df = extract_release_metadata(logger, releases)

    # Insert extension, publisher, and release data into the database
    upsert_publishers(logger, connection, publishers_df)
    upsert_extensions(logger, connection, extensions_df)
    upsert_releases(logger, connection, releases_df)

    # Upload extension files to S3
    s3_client = boto3.client("s3")
    combined_df = combine_dataframes(extensions_df, publishers_df, releases_df)
    upload_all_extensions_to_s3(logger, session, connection, s3_client, combined_df)

    # Check the consistency of data in the database and S3
    validate_data_consistency(logger, connection, s3_client)

    # Close all connections
    s3_client.close()
    connection.close()
    session.close()


if __name__ == "__main__":
    main()
