"""Fetches extension releases from the VSCode Marketplace"""

import time
from logging import Logger
import requests
import pandas as pd
import psycopg2
from dateutil import parser

from util import (
    upsert_data,
    clean_dataframe,
    connect_to_database,
    select_extensions,
    select_latest_releases,
)


def get_extension_releases(
    logger: Logger,
    extension_identifier: str,
    page_number: int = 1,
    releases_page_size: int = 100,
) -> dict:
    """Fetches releases metadata for a given extension from the VSCode Marketplace"""

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
                "pageSize": releases_page_size,
                "pageNumber": page_number,
            },
        ],
        "flags": 0x1,  # Include versions
    }

    response = requests.post(
        "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery",
        headers={"accept": "application/json;api-version=7.2-preview.1;"},
        json=json_data,
        timeout=5,
    )

    if response.status_code == 200:
        releases = response.json()["results"][0]["extensions"][0]["versions"]
        logger.info(
            "get_extension_releases: Fetched extension releases for extension %s",
            extension_identifier,
        )
        return releases

    logger.error(
        "get_extension_releases: Error fetching releases for extension %s "
        "from page number %d: status code %d",
        extension_identifier,
        page_number,
        response.status_code,
    )
    return {}


def get_all_releases(
    logger: Logger,
    extensions_df: pd.DataFrame,
    releases_df: pd.DataFrame,
) -> dict:
    """Fetches all release metadata from the VSCode Marketplace"""

    all_releases = {}
    extension_data = list(
        zip(
            extensions_df["extension_id"],
            extensions_df["extension_identifier"],
            extensions_df["latest_release_version"],
        )
    )

    for extension_id, extension_identifier, extensions_latest_version in extension_data:
        releases_latest_version = releases_df[
            releases_df["extension_id"] == extension_id
        ]["version"]

        # Check if the latest release has already been fetched for the extension in a previous run
        if (
            not releases_latest_version.empty
            and extensions_latest_version == releases_latest_version.iloc[0]
        ):
            logger.info(
                "get_all_releases: Skipped fetching the releases for %s "
                "since they have already been retrieved",
                extension_identifier,
            )
            continue

        time.sleep(1)

        releases = get_extension_releases(logger, extension_identifier)
        all_releases[extension_id] = releases

    return all_releases


def extract_release_metadata(logger: Logger, releases: list) -> pd.DataFrame:
    """Extracts the relevant release information from the raw data"""

    release_metadata = []
    release_ids = set()

    for extension_id in releases:
        extension_releases = releases[extension_id]

        for extension_release in extension_releases:
            extension_version = extension_release["version"]
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

            release_metadata.append(
                {
                    "release_id": release_id,
                    "version": extension_version,
                    "flags": extension_release["flags"],
                    "last_updated": parser.isoparse(extension_release["lastUpdated"]),
                    "extension_id": extension_id,
                }
            )

    return pd.DataFrame(
        release_metadata,
        columns=["release_id", "version", "extension_id", "flags", "last_updated"],
    )


def upsert_releases(
    logger: Logger,
    connection: psycopg2.extensions.connection,
    releases_df: pd.DataFrame,
    batch_size: int = 5000,
) -> None:
    """Upserts the given releases to the database in batches"""

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
            row["last_updated"] if not pd.isna(row["last_updated"]) else None,
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


def fetch_releases(logger: Logger):
    """Orchestrates the retrieval of extension release data"""

    # Setup
    connection = connect_to_database(logger)

    # Fetch the existing data from the database
    extensions_df = select_extensions(logger, connection)
    releases_df = select_latest_releases(logger, connection)

    # Fetch data from VSCode Marketplace
    releases = get_all_releases(logger, extensions_df, releases_df)
    releases_df = extract_release_metadata(logger, releases)

    # Upsert retrieved data to the database
    releases_df = clean_dataframe(releases_df)
    upsert_releases(logger, connection, releases_df)

    # Close
    connection.close()
