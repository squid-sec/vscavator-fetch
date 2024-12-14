"""
TODO
"""

import os
import logging
import logging.config
import boto3
import requests
from dateutil import parser
from packaging import version
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

load_dotenv()

s3 = boto3.client("s3")

EXTENSIONS_URL = "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery"
DOWNLOAD_URL = (
    "https://marketplace.visualstudio.com/_apis/public/gallery/"
    "publishers/{publisher}/vsextensions/{name}/{version}/vspackage"
)
HEADERS = {
    "Content-Type": "application/json",
    "accept": "application/json;api-version=7.2-preview.1;excludeUrls=true",
}
REQUESTS_TIMEOUT = 10
EXTENSIONS_PAGE_SIZE = 2
EXTENSIONS_LAST_PAGE_NUMBER = 3

CREATE_EXTENSIONS_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS extensions (
        extension_id VARCHAR(255) PRIMARY KEY NOT NULL,
        extension_name VARCHAR(255) NOT NULL,
        display_name VARCHAR(255) NOT NULL,
        flags VARCHAR(255) ARRAY,
        last_updated DATE NOT NULL,
        published_date DATE NOT NULL,
        release_date DATE NOT NULL,
        short_description VARCHAR(255) NOT NULL,
        latest_release_version VARCHAR(255) NOT NULL,
        publisher_id VARCHAR(255) NOT NULL,
        extension_identifier VARCHAR(255) NOT NULL,
        FOREIGN KEY (publisher_id) REFERENCES publishers (publisher_id) ON DELETE CASCADE
    );
"""
CREATE_PUBLISHERS_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS publishers (
        publisher_id VARCHAR(255) PRIMARY KEY NOT NULL,
        publisher_name VARCHAR(255) NOT NULL,
        display_name VARCHAR(255) NOT NULL,
        flags VARCHAR(255) ARRAY,
        domain VARCHAR(255),
        is_domain_verified BOOLEAN NOT NULL
    );
"""
CREATE_RELEASES_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS releases (
        release_id VARCHAR(255) PRIMARY KEY NOT NULL,
        extension_id VARCHAR(255) NOT NULL,
        version VARCHAR(255) NOT NULL,
        last_updated DATE NOT NULL,
        flags VARCHAR(255) ARRAY,
        uploaded_to_s3 BOOLEAN NOT NULL DEFAULT FALSE,
        FOREIGN KEY (extension_id) REFERENCES extensions (extension_id) ON DELETE CASCADE
    );
"""

def get_extensions(
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
        logging.info(
            "Fetched extensions from page number %d with page size %d",
            page_number, page_size
        )
        return results

    logging.error(
        "Error fetching extensions from page number %d with page size %d: status code %d",
        page_number, page_size, response.status_code
    )
    return []

def get_all_extensions(
        page_size: int = EXTENSIONS_PAGE_SIZE,
        last_page_number: int = EXTENSIONS_LAST_PAGE_NUMBER
    ) -> list:
    """
    TODO
    """

    all_extensions = []
    for page_number in range(1, last_page_number + 1):
        extensions = get_extensions(page_number, page_size)
        all_extensions.extend(extensions)
    return all_extensions

def upload_extension_to_s3(
        connection: psycopg2.extensions.connection,
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

    response = requests.get(url, stream=True, timeout=REQUESTS_TIMEOUT)
    if response.status_code == 200:
        s3_key = f"extensions/{publisher_name}/{extension_name}/{extension_version}.vsix"
        try:
            s3.upload_fileobj(response.raw, os.getenv("S3_BUCKET_NAME"), s3_key)
            logging.info(
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
                logging.info(
                    "Updated uploaded_to_s3 status to TRUE for version %s of extension %s",
                    extension_version, extension_name
                )
            else:
                logging.error(
                    "Failed to update uploaded_to_s3 status to True for version %s of extension %s",
                    extension_version, extension_name
                )

            cursor.close()
        except Exception as e: # pylint: disable=broad-exception-caught
            logging.error(
                "Error uploading extension %s version %s by publisher %s to S3: %s",
                extension_name, extension_version, publisher_name, e
            )
    else:
        logging.error(
            "Error downloading extension %s version %s by publisher %s from marketplace: "
            "status code %d",
            extension_name, version, publisher_name, response.status_code
        )

def upload_all_extensions_to_s3(
        connection: psycopg2.extensions.connection,
        combined_df: pd.DataFrame
    ) -> None:
    """
    TODO
    """

    for _, row in combined_df.iterrows():
        publisher_name = row["publisher_name"]
        extension_name = row["extension_name"]
        extension_id = row["extension_id"]
        extension_version = row["version"]

        if is_uploaded_to_s3(connection, extension_id, extension_version):
            logging.info(
                "Skipped uploading version %s of extension %s to S3 since it has already been "
                "uploaded",
                extension_version, extension_name
            )
            continue

        upload_extension_to_s3(
            connection, extension_id, publisher_name, extension_name, extension_version
        )

def get_extension_releases(
        extension_identifier: str
    ) -> list:
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
        logging.info(
            "Fetched extension releases for extension %s",
            extension_identifier
        )
        return results

    logging.error(
        "Error fetching extension releases for extension %s: %d",
        extension_identifier, response.status_code
    )
    return []

def extract_publisher_metadata(
        extensions: list
    ) -> pd.DataFrame:
    """
    TODO
    """

    publishers_metadata = []

    for extension in extensions:
        publisher_metadata = extension["publisher"]
        publishers_metadata.append({
            "publisher_id": publisher_metadata["publisherId"],
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
        extension_releases: list
    ) -> pd.DataFrame:
    """
    TODO
    """

    releases = []

    extension_id = extension_releases["extensionId"]
    extension_versions = extension_releases["versions"]
    for extension in extension_versions:
        extension_version = extension["version"]
        release_id = extension_id + "-" + extension_version

        releases.append({
            "release_id": release_id,
            "version": extension_version,
            "flags": extension["flags"].split(", "),
            "last_updated": parser.isoparse(extension["lastUpdated"]),
            "extension_id": extension_id,
        })

    return pd.DataFrame(releases)

def upsert_data(
        connection: psycopg2.extensions.connection,
        table_name: str,
        upsert_data_query: str,
        data: list
    ) -> None:
    """
    TODO
    """

    cursor = connection.cursor()
    execute_values(cursor, upsert_data_query, data)

    if cursor.rowcount > 0:
        connection.commit()
        logging.info(
            "Upserted %d rows of %s data to the database",
            len(data), table_name
        )
    else:
        logging.error(
            "Error upserting %d rows of %s data to the database: {str(e)}",
            len(data), table_name
        )

    cursor.close()

def upsert_extensions(
        connection: psycopg2.extensions.connection,
        extensions_df: pd.DataFrame,
        batch_size: int = 5000
    ) -> None:
    """
    TODO
    """

    upsert_query = """
        INSERT INTO extensions (
            extension_id, extension_name, display_name, flags, last_updated, published_date, release_date, short_description, latest_release_version, publisher_id, extension_identifier
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
            publisher_id = EXCLUDED.publisher_id,
            extension_identifier = EXCLUDED.extension_identifier
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
            row["publisher_id"],
            row["extension_identifier"]
        ) for _, row in extensions_df.iterrows()
    ]

    for i in range(0, len(values), batch_size):
        batch = values[i:i + batch_size]
        upsert_data(connection, "extensions", upsert_query, batch)
        logging.info(
            "Upserted extensions batch %d of %d rows",
            i // batch_size + 1, len(batch)
        )

def upsert_publishers(
        connection: psycopg2.extensions.connection,
        publishers_df: pd.DataFrame,
        batch_size: int = 5000
    ) -> None:
    """
    TODO
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
            is_domain_verified = EXCLUDED.is_domain_verified
    """

    values = [
        (
            row["publisher_id"],
            row["publisher_name"],
            row["display_name"],
            row["flags"],
            row["domain"],
            row["is_domain_verified"]
        ) for _, row in publishers_df.iterrows()
    ]

    for i in range(0, len(values), batch_size):
        batch = values[i:i + batch_size]
        upsert_data(connection, "publishers", upsert_query, batch)
        logging.info(
            "Upserted publishers batch %d of %d rows",
            i // batch_size + 1, len(batch)
        )

def upsert_releases(
        connection: psycopg2.extensions.connection,
        releases_df: pd.DataFrame,
        batch_size: int = 5000
    ) -> None:
    """
    TODO
    """

    upsert_query = """
        INSERT INTO releases (
            release_id, version, extension_id, flags, last_updated
        ) VALUES %s
        ON CONFLICT (release_id) DO UPDATE SET
            version = EXCLUDED.version,
            extension_id = EXCLUDED.extension_id,
            flags = EXCLUDED.flags,
            last_updated = EXCLUDED.last_updated
    """

    values = [
        (
            row["release_id"],
            row["version"],
            row["extension_id"],
            row["flags"],
            row["last_updated"],
        ) for _, row in releases_df.iterrows()
    ]

    for i in range(0, len(values), batch_size):
        batch = values[i:i + batch_size]
        upsert_data(connection, "releases", upsert_query, values)
        logging.info(
            "Upserted releases batch %d of %d rows",
            i // batch_size + 1, len(batch)
        )

def create_table(
        connection: psycopg2.extensions.connection,
        table_name: str,
        create_table_query: str
    ) -> None:
    """
    TODO
    """

    if connection is None:
        logging.error("Failed to create %s table: no database connection", table_name)
        return

    cursor = connection.cursor()
    cursor.execute(create_table_query)

    connection.commit()
    logging.info(
        "Created %s table",
        table_name
    )
    cursor.close()

def connect_to_database() -> psycopg2.extensions.connection:
    """
    TODO
    """

    connection = psycopg2.connect(
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT")
    )

    if connection:
        logging.info(
            "Connected to database %s on host %s:%s",
            os.getenv("PG_DATABASE"), os.getenv("PG_HOST"), os.getenv("PG_PORT")
        )
        return connection

    logging.critical(
        "Failed to connect to database %s on host %s",
        os.getenv("PG_DATABASE"), os.getenv("PG_HOST")
    )
    return None

def create_all_tables(
        connection: psycopg2.extensions.connection
    ) -> None:
    """
    TODO
    """

    create_table(connection, "publishers", CREATE_PUBLISHERS_TABLE_QUERY)
    create_table(connection, "extensions", CREATE_EXTENSIONS_TABLE_QUERY)
    create_table(connection, "releases", CREATE_RELEASES_TABLE_QUERY)

def get_old_latest_release_version(
        connection: psycopg2.extensions.connection,
        extension_identifier: str
    ) -> str:
    """
    TODO
    """

    query = f"""
        SELECT latest_release_version
        FROM extensions
        WHERE extension_identifier = '{extension_identifier}';
    """

    cursor = connection.cursor()
    cursor.execute(query)

    if cursor.rowcount > 0:
        result = cursor.fetchone()
        logging.info(
            "Fetched latest release version from the extensions table for extension %s",
            extension_identifier
        )
        cursor.close()
        return result[0]

    logging.info(
        "No latest release version from the extensions table for extension %s was found",
        extension_identifier
    )
    cursor.close()
    return None

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

    return None

def is_uploaded_to_s3(
        connection: psycopg2.extensions.connection,
        extension_id: str,
        extension_version: str
    ) -> bool:
    """
    TODO
    """

    query = f"""
        SELECT uploaded_to_s3
        FROM releases
        WHERE extension_id = '{extension_id}' AND version = '{extension_version}';
    """

    cursor = connection.cursor()
    cursor.execute(query)
    if cursor.rowcount > 0:
        result = cursor.fetchone()
        logging.info(
            "Fetched upload status for version %s of extension %s",
            extension_version, extension_id
        )
        cursor.close()
        return result[0]

    logging.info(
        "No S3 upload status for version %s of extension %s was found",
        extension_version, extension_id
    )
    cursor.close()
    return False

def combine_dataframes(
        extensions_df: pd.DataFrame,
        publishers_df: pd.DataFrame,
        releases_df: pd.DataFrame
    ) -> pd.DataFrame:
    """
    TODO
    """

    releases_extensions_df = releases_df.merge(extensions_df, on="extension_id", how="inner")
    combined_df = releases_extensions_df.merge(publishers_df, on="publisher_id", how="inner")
    return combined_df

def get_all_releases(
        connection: psycopg2.extensions.connection,
        extensions_df: pd.DataFrame
    )-> pd.DataFrame:
    """
    TODO
    """

    extension_identifiers = extensions_df["extension_identifier"].tolist()
    releases_df = pd.DataFrame(
        columns=["release_id", "version", "extension_id", "flags", "last_updated"]
    )

    for extension_identifier in extension_identifiers:
        old_latest_release_version = get_old_latest_release_version(
            connection, extension_identifier
        )
        new_latest_release_version = get_new_latest_release_version(
            extensions_df, extension_identifier
        )

        if old_latest_release_version == new_latest_release_version:
            logging.info(
                "Skipped fetching the releases for %s since they have already been retrieved",
                extension_identifier
            )
            continue

        extension_releases = get_extension_releases(extension_identifier)
        extension_releases_df = extract_release_metadata(extension_releases)
        releases_df = pd.concat([releases_df, extension_releases_df], ignore_index=True)

    return releases_df

def main() -> None:
    """
    TODO
    """

    connection = connect_to_database()
    create_all_tables(connection)

    extensions = get_all_extensions()
    extensions_df = extract_extension_metadata(extensions)
    publishers_df = extract_publisher_metadata(extensions)

    releases_df = get_all_releases(connection, extensions_df)

    upsert_publishers(connection, publishers_df)
    upsert_extensions(connection, extensions_df)
    upsert_releases(connection, releases_df)

    combined_df = combine_dataframes(extensions_df, publishers_df, releases_df)
    upload_all_extensions_to_s3(connection, combined_df)

    connection.close()

if __name__ == "__main__":
    main()
