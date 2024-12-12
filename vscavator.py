import logging.config
import requests
import os
import zipfile
from dateutil import parser
import logging
from packaging import version
import pandas as pd
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extras import execute_values


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

load_dotenv()

EXTENSIONS_URL = "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery"
DOWNLOAD_URL = "https://marketplace.visualstudio.com/_apis/public/gallery/publishers/{publisher}/vsextensions/{name}/{version}/vspackage"
HEADERS = {
    "Content-Type": "application/json",
    "accept": "application/json;api-version=7.2-preview.1;excludeUrls=true",
}
LAST_PAGE_NUMBER = 1
PAGE_SIZE = 2

CREATE_EXTENSIONS_TABLE_QUERY = """
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
"""


def get_extensions(page_number, page_size):
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

    try:
        response = requests.post(EXTENSIONS_URL, headers=HEADERS, json=payload)
        results = response.json()["results"][0]["extensions"]
        logging.info(f"fetched extensions from page number {str(page_number)} with page size {str(page_size)}")
        return results
    except Exception as e:
        logging.error(f"error while fetching extensions from page number {str(page_number)} with page size {str(page_size)}: {str(e)}")
        return []

def download_extension(publisher, name, version):
    url = DOWNLOAD_URL.format(publisher=publisher, name=name, version=version)

    response = requests.get(url, stream=True)
    if response.status_code == 200:
        file_path = os.path.join("extensions/zipped", f"{publisher}-{name}-{version}.vsix")
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded: {file_path}")
    else:
        print(f"Failed to download {publisher}/{name}@{version}")

def unzip_file(file, extension_id):
    extract_to_folder = f"extensions/unzipped/{extension_id}"
    with zipfile.ZipFile(file, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)

def get_extension_releases(extension_identifier):
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

    try:
        response = requests.post(EXTENSIONS_URL, json=json_data, headers=HEADERS)
        results = response.json()["results"][0]["extensions"][0]
        logging.info(f"fetched extension releases for extension {extension_identifier}")
        return results
    except Exception as e:
        logging.error(f"error while fetching extension releases for extension {extension_identifier}: {str(e)}")
        return []

def extract_publisher_metadata(extensions):
    publishers_metadata = []

    for extension in extensions:
        publisher_metadata = extension["publisher"]

        publisher_id = publisher_metadata["publisherId"]
        publisher_name = publisher_metadata["publisherName"]
        display_name = publisher_metadata["displayName"]
        flags = publisher_metadata["flags"].split(", ")
        domain = publisher_metadata["domain"]
        is_domain_verified = publisher_metadata["isDomainVerified"]

        publishers_metadata.append({
            "publisher_id": publisher_id,
            "publisher_name": publisher_name,
            "display_name": display_name,
            "flags": flags,
            "domain": domain,
            "is_domain_verified": is_domain_verified
        })

    return pd.DataFrame(publishers_metadata)

def extract_extension_metadata(extensions):
    extensions_metadata = []

    for extension in extensions:
        extension_id = extension["extensionId"]
        extension_name = extension["extensionName"]
        display_name = extension["displayName"]
        flags = extension["flags"].split(", ")
        last_updated = parser.isoparse(extension["lastUpdated"])
        published_date = parser.isoparse(extension["publishedDate"])
        release_date = parser.isoparse(extension["releaseDate"])
        short_description = extension["shortDescription"]
        publisher_id = extension["publisher"]["publisherId"]
        publisher_name = extension["publisher"]["publisherName"]
        extension_identifier = f"{publisher_name}.{extension_name}"

        get_latest_version = lambda v: max(v, key=lambda x: version.parse(x["version"]))["version"]
        versions = extension["versions"]
        latest_release_version = get_latest_version(versions)

        extensions_metadata.append({
            "extensionId": extension_id,
            "extensionName": extension_name,
            "displayName": display_name,
            "flags": flags,
            "lastUpdated": last_updated,
            "publishedDate": published_date,
            "releaseDate": release_date,
            "shortDescription": short_description,
            "latestReleaseVersion": latest_release_version,
            "publisherId": publisher_id,
            "extensionIdentifier": extension_identifier
        })

    return pd.DataFrame(extensions_metadata)

def extract_release_metadata(extension_releases):
    releases = []

    extension_id = extension_releases["extensionId"]
    extension_versions = extension_releases["versions"]
    for extension_version in extension_versions:
        version = extension_version["version"]
        flags = extension_version["flags"].split(", ")
        last_updated = parser.isoparse(extension_version["lastUpdated"])

        releases.append({
            "version": version,
            "flags": flags,
            "lastUpdated": last_updated,
            "extensionId": extension_id
        })

    return pd.DataFrame(releases)


def upsert_publishers(connection, publishers_df):
    try:
        with connection.cursor() as cursor:
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

            execute_values(cursor, upsert_query, values)
            connection.commit()
            logging.info("Successfully upserted publishers data to the database.")
    except Exception as e:
        logging.error(f"Error upserting publishers data to the database: {str(e)}")

def create_table(connection, table_name, create_table_query):
    if connection is None:
        logging.error("database connection to create table is none")
        return

    cursor = connection.cursor()

    try:
        cursor.execute(create_table_query)
        logging.info(f"executed create {table_name} table query")
        connection.commit()
        logging.info(f"commited create {table_name} table query")
    except Exception as e:
        connection.rollback()
        logging.error(f"rolled back create {table_name} table query: {str(e)}")
    finally:
        cursor.close()
        logging.info(f"closed create {table_name} table query cursor")

def connect_to_database():
    try:
        connection = psycopg2.connect(
            dbname=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT")
        )
        logging.info(f"connected to database {os.getenv("PG_DATABASE")} on host {os.getenv("PG_HOST")}")
        return connection
    except Exception as e:
        logging.error(f"failed to connect to database {os.getenv("PG_DATABASE")} on host {os.getenv("PG_HOST")}: {str(e)}")
        return None
    
def create_tables(connection):
    pass

def main():
    connection = connect_to_database()
    if connection is None:
        return

    create_tables(connection)


    all_extensions = []
    for page_number in range(1, LAST_PAGE_NUMBER + 1):
        extensions = get_extensions(page_number, PAGE_SIZE)
        all_extensions.extend(extensions)

    extensions_df = extract_extension_metadata(all_extensions)
    publishers_df = extract_publisher_metadata(all_extensions)

    extension_identifiers = extensions_df["extensionIdentifier"].tolist()

    releases_df = pd.DataFrame()
    for extension_identifier in extension_identifiers:
        # TODO: Check if the latest release has already been fetched and if it has continue to the next extension
        extension_releases = get_extension_releases(extension_identifier)
        extension_releases_df = extract_release_metadata(extension_releases)
        releases_df = pd.concat([releases_df, extension_releases_df], ignore_index=True)

if __name__ == "__main__":
    main()
