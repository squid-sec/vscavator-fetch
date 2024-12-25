"""
TODO
"""

import time
from logging import Logger
import requests
import pandas as pd
from dateutil import parser
import psycopg2

from util import upsert_data, clean_dataframe, connect_to_database


def get_total_number_of_extensions(logger: Logger) -> int:
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
        "flags": 0x100,  # Include statistics
    }

    response = requests.post(
        "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery",
        headers={"accept": "application/json;api-version=7.2-preview.1;"},
        json=payload,
        timeout=5,
    )
    if response.status_code == 200:
        result_metadata = response.json()["results"][0]["resultMetadata"]

        total_count = None
        for metadata in result_metadata:
            if metadata["metadataType"] == "ResultCount":
                for item in metadata["metadataItems"]:
                    if item["name"] == "TotalCount":
                        total_count = item["count"]
                        break

        logger.info(
            "get_total_number_of_extensions: total number of extensions is %d",
            total_count,
        )

        return total_count

    logger.critical(
        "get_total_number_of_extensions: Error fetching number of extensions: "
        "status code %d",
        response.status_code,
    )
    return 0


def calculate_number_of_extension_pages(
    num_extensions: int, extensions_page_size: int = 100
) -> int:
    """
    calculate_number_of_extension_pages calculates the number of extension pages to fetch
    """

    return num_extensions // extensions_page_size + 1


def get_extensions(
    logger: Logger,
    page_number: int,
    extensions_page_size: int = 100,
) -> list:
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
                "pageSize": extensions_page_size,
                "pageNumber": page_number,
                "sortBy": 2,  # Title
                "sortOrder": 1,  # Ascending
            },
        ],
        "flags": 0x100,  # Include statistics
    }

    response = requests.post(
        "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery",
        headers={"accept": "application/json;api-version=7.2-preview.1;"},
        json=payload,
        timeout=5,
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


def get_all_extensions(
    logger: Logger,
    last_page_number: int,
) -> list:
    """
    get_all_extensions fetches all extension metadata from the VSCode Marketplace
    """

    all_extensions = []

    for page_number in range(1, last_page_number + 1):
        time.sleep(1)
        extensions = get_extensions(logger, page_number)
        all_extensions.extend(extensions)

    return all_extensions


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
        github_url = extract_extension_github_url(properties)
        statistics = extract_extension_statistics(extension["statistics"])

        extensions_metadata.append(
            {
                "extension_id": extension_id,
                "extension_name": extension["extensionName"],
                "display_name": extension["displayName"],
                "flags": extension["flags"],
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
                "average_rating": statistics["averagerating"],
                "rating_count": statistics["ratingcount"],
                "trending_daily": statistics["trendingdaily"],
                "trending_monthly": statistics["trendingmonthly"],
                "trending_weekly": statistics["trendingweekly"],
                "update_count": statistics["updateCount"],
                "weighted_rating": statistics["weightedRating"],
                "download_count": statistics["downloadCount"],
            }
        )

    return pd.DataFrame(extensions_metadata)


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
                "flags": publisher_metadata["flags"],
                "domain": publisher_metadata["domain"],
                "is_domain_verified": publisher_metadata["isDomainVerified"],
            }
        )

    return pd.DataFrame(publishers_metadata)


def extract_extension_statistics(statistics: list) -> dict:
    """
    extract_extension_statistics finds the extension statistics
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


def extract_extension_github_url(properties: list) -> str:
    """
    extract_extension_github_url finds the GitHub URL of the extension
    """

    github_url = ""
    for extension_property in properties:
        if extension_property["key"] == "Microsoft.VisualStudio.Services.Links.GitHub":
            github_url = extension_property["value"]
            break
    return github_url


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
            extension_identifier, github_url, install, average_rating, rating_count, trending_daily, trending_monthly, 
            trending_weekly, update_count, weighted_rating, download_count
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
            github_url = EXCLUDED.github_url,
            install = EXCLUDED.install,
            average_rating = EXCLUDED.average_rating,
            rating_count = EXCLUDED.rating_count,
            trending_daily = EXCLUDED.trending_daily,
            trending_monthly = EXCLUDED.trending_monthly,
            trending_weekly = EXCLUDED.trending_weekly,
            update_count = EXCLUDED.update_count,
            weighted_rating = EXCLUDED.weighted_rating,
            download_count = EXCLUDED.download_count;
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
            row["github_url"],
            row["install"],
            row["average_rating"],
            row["rating_count"],
            row["trending_daily"],
            row["trending_monthly"],
            row["trending_weekly"],
            row["update_count"],
            row["weighted_rating"],
            row["download_count"],
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


def fetch_extensions_and_publishers(logger: Logger):
    """
    fetch_extensions_and_publishers
    """

    connection = connect_to_database(logger)

    num_total_extensions = get_total_number_of_extensions(logger)
    num_extension_pages = calculate_number_of_extension_pages(num_total_extensions)

    extensions = get_all_extensions(logger, num_extension_pages)
    extensions_df = extract_extension_metadata(extensions)
    publishers_df = extract_publisher_metadata(logger, extensions)

    publishers_df = clean_dataframe(publishers_df)
    extensions_df = clean_dataframe(extensions_df)

    upsert_publishers(logger, connection, publishers_df)
    upsert_extensions(logger, connection, extensions_df)

    connection.close()
