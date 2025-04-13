"""Fetches extension reviews from the VSCode Marketplace"""

from logging import Logger
import requests
import pandas as pd
import psycopg2

from util import (
    upsert_data,
    clean_dataframe,
    connect_to_database,
    combine_dataframes,
    select_extensions,
    select_publishers,
)


def get_extension_reviews(
    logger: Logger,
    publisher_name: str,
    extension_name: str,
) -> list:
    """Fetches review metadata for a given extension from the VSCode Marketplace"""

    response = requests.get(
        f"https://marketplace.visualstudio.com/_apis/public/gallery/"
        f"publishers/{publisher_name}/extensions/{extension_name}/reviews?count=100",
        headers={"accept": "application/json;api-version=7.2-preview.1;"},
        timeout=5,
    )

    if response.status_code == 200:
        reviews = response.json()["reviews"]
        logger.info(
            "get_extension_reviews: Fetched extension reviews for extension %s",
            extension_name,
        )
        return reviews

    logger.error(
        "get_extension_reviews: Error fetching reviews for extension %s: "
        "status code %d",
        extension_name,
        response.status_code,
    )
    return []


def get_all_reviews(
    logger: Logger,
    combined_df: pd.DataFrame,
) -> dict:
    """Fetches all review metadata from the VSCode Marketplace"""

    all_reviews = {}

    for _, row in combined_df.iterrows():
        extension_id = row["extension_id"]
        publisher_name = row["publisher_name"]
        extension_name = row["extension_name"]

        reviews = get_extension_reviews(logger, publisher_name, extension_name)
        all_reviews[extension_id] = reviews

    return all_reviews


def extract_review_metadata(extension_reviews: dict) -> pd.DataFrame:
    """Extracts the relevant review information from the given raw data"""

    review_metadata = []

    for extension_id in extension_reviews:
        for review in extension_reviews[extension_id]:
            review_metadata.append(
                {
                    "review_id": review["id"],
                    "extension_id": extension_id,
                    "user_id": review["userId"],
                    "user_display_name": review["userDisplayName"],
                    "updated_date": review["updatedDate"],
                    "rating": review["rating"],
                    "text": review.get("text", ""),
                    "product_version": review["productVersion"],
                }
            )

    return pd.DataFrame(review_metadata)


def upsert_reviews(
    logger: Logger,
    connection: psycopg2.extensions.connection,
    reviews_df: pd.DataFrame,
    batch_size: int = 5000,
) -> None:
    """Upserts the given reviews to the database in batches"""

    upsert_query = """
        INSERT INTO reviews (
            review_id, extension_id, user_id, user_display_name, updated_date, rating, text, product_version
        ) VALUES %s
        ON CONFLICT (review_id) DO UPDATE SET
            extension_id = EXCLUDED.extension_id,
            user_id = EXCLUDED.user_id,
            user_display_name = EXCLUDED.user_display_name,
            updated_date = EXCLUDED.updated_date,
            rating = EXCLUDED.rating,
            text = EXCLUDED.text,
            product_version = EXCLUDED.product_version;
    """

    values = [
        (
            row["review_id"],
            row["extension_id"],
            row["user_id"],
            row["user_display_name"],
            row["updated_date"],
            row["rating"],
            row["text"],
            row["product_version"],
        )
        for _, row in reviews_df.iterrows()
    ]

    for i in range(0, len(values), batch_size):
        batch = values[i : i + batch_size]
        upsert_data(logger, connection, "reviews", upsert_query, values)
        logger.info(
            "upsert_reviews: Upserted reviews batch %d of %d rows",
            i // batch_size + 1,
            len(batch),
        )


def fetch_reviews(logger: Logger):
    """Orchestrates the retrieval of extension review data"""

    # Setup
    connection = connect_to_database(logger)

    # Fetch the existing data from the database
    extensions_df = select_extensions(logger, connection)
    publishers_df = select_publishers(logger, connection)
    extensions_publishers_df = combine_dataframes(
        [extensions_df, publishers_df], ["publisher_id"]
    )

    # Fetch data from VSCode Marketplace
    reviews = get_all_reviews(logger, extensions_publishers_df)
    reviews_df = extract_review_metadata(reviews)

    # Upsert retrieved data to the database
    reviews_df = clean_dataframe(reviews_df)
    upsert_reviews(logger, connection, reviews_df)

    # Close
    connection.close()
