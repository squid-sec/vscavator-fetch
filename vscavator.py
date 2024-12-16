"""
TODO
"""

import time
import logging
import logging.config
from logging import Logger
import boto3
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

EXTENSIONS_URL = "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery"
HEADERS = {
    "Content-Type": "application/json",
    "accept": "application/json;api-version=7.2-preview.1;excludeUrls=true",
}
REQUESTS_TIMEOUT = 10
EXTENSIONS_PAGE_SIZE = 30
EXTENSIONS_LAST_PAGE_NUMBER = 30
REQUESTS_SLEEP = 60

def get_extensions(
    logger: Logger,
    page_number: int,
    page_size: int
) -> list:
    """
    TODO
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

def get_all_extensions(
    logger: Logger,
    page_size: int = EXTENSIONS_PAGE_SIZE,
    last_page_number: int = EXTENSIONS_LAST_PAGE_NUMBER
) -> list:
    """
    TODO
    """

    all_extensions = []
    for page_number in range(1, last_page_number + 1):
        extensions = get_extensions(logger, page_number, page_size)
        all_extensions.extend(extensions)
    return all_extensions

def get_extension_releases(
    logger: Logger,
    extension_identifier: str
) -> dict:
    """
    TODO
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

def extract_publisher_metadata(
    extensions: list
) -> pd.DataFrame:
    """
    TODO
    """

    publishers_metadata = []
    unique_publishers = set()

    for extension in extensions:
        publisher_metadata = extension["publisher"]
        publisher_id = publisher_metadata["publisherId"]
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

def get_latest_version(
    versions: list
) -> str:
    """
    TODO
    """

    return max(versions, key=lambda x: version.parse(x["version"]))["version"]

def extract_extension_metadata(
    extensions: list
) -> pd.DataFrame:
    """
    TODO
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
    extension_releases: dict
) -> pd.DataFrame:
    """
    TODO
    """

    if "extensionId" not in extension_releases:
        return pd.DataFrame()

    releases = []
    release_ids = set()

    extension_id = extension_releases["extensionId"]
    extension_versions = extension_releases["versions"]
    for extension in extension_versions:
        extension_version = extension["version"]
        release_id = extension_id + "-" + extension_version
        if release_id in release_ids:
            continue
        release_ids.add(release_id)

        releases.append({
            "release_id": release_id,
            "version": extension_version,
            "flags": extension["flags"].split(", "),
            "last_updated": parser.isoparse(extension["lastUpdated"]),
            "extension_id": extension_id,
        })

    return pd.DataFrame(releases)

def get_new_latest_release_version(
    df: pd.DataFrame,
    extension_identifier: str
) -> str:
    """
    TODO
    """

    latest_release_version = df.loc[
        df["extension_identifier"] == extension_identifier, "latest_release_version"
    ]

    if not latest_release_version.empty:
        return latest_release_version.iloc[-1]

    return ""

def get_all_releases(
    logger: Logger,
    connection: psycopg2.extensions.connection,
    extensions_df: pd.DataFrame
) -> pd.DataFrame:
    """
    TODO
    """

    extension_identifiers = extensions_df["extension_identifier"].tolist()
    releases_df = pd.DataFrame(
        columns=["release_id", "version", "extension_id", "flags", "last_updated"]
    )

    for extension_identifier in extension_identifiers:
        old_latest_release_version = get_old_latest_release_version(
            logger, connection, extension_identifier
        )
        new_latest_release_version = get_new_latest_release_version(
            extensions_df, extension_identifier
        )

        if old_latest_release_version == new_latest_release_version:
            logger.info(
                "Skipped fetching the releases for %s since they have already been retrieved",
                extension_identifier
            )
            continue

        extension_releases = get_extension_releases(logger, extension_identifier)
        extension_releases_df = extract_release_metadata(extension_releases)
        releases_df = pd.concat([releases_df, extension_releases_df], ignore_index=True)

    return releases_df

def validate_data_consistency(
    logger,
    connection,
    s3_client
) -> None:
    """
    TODO
    """

    object_keys = get_all_object_keys(s3_client)
    object_keys_df = object_keys_to_dataframe(object_keys)

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

        if db_uploaded_to_s3 != s3_uploaded_to_s3:
            logger.error(
                "Publisher %s, extension %s, version %s: "
                "database state uploaded_to_s3: %s, while S3 state uploaded_to_s3: %s",
                publisher_name, extension_name, extension_version, db_uploaded_to_s3,
                s3_uploaded_to_s3
            )

def main() -> None:
    """
    TODO
    """

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    vscavator_logger = logging.getLogger("vscavator")
    load_dotenv()

    connection = connect_to_database(vscavator_logger)
    create_all_tables(vscavator_logger, connection)

    extensions = get_all_extensions(vscavator_logger)
    extensions_df = extract_extension_metadata(extensions)
    publishers_df = extract_publisher_metadata(extensions)

    releases_df = get_all_releases(vscavator_logger, connection, extensions_df)

    upsert_publishers(vscavator_logger, connection, publishers_df)
    upsert_extensions(vscavator_logger, connection, extensions_df)
    upsert_releases(vscavator_logger, connection, releases_df)

    combined_df = combine_dataframes(extensions_df, publishers_df, releases_df)

    s3_client = boto3.client("s3")
    upload_all_extensions_to_s3(vscavator_logger, connection, s3_client, combined_df)

    validate_data_consistency(vscavator_logger, connection, s3_client)

    s3_client.close()
    connection.close()

if __name__ == "__main__":
    main()
