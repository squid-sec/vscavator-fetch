"""
TODO
"""

import logging
import json

from util import (
    connect_to_database,
    select_extensions,
    select_publishers,
    select_latest_releases,
    clean_dataframe,
    combine_dataframes,
)
from fetch_extensions import (
    get_total_number_of_extensions,
    calculate_number_of_extension_pages,
    get_all_extensions,
    extract_extension_metadata,
    extract_publisher_metadata,
)
from fetch_releases import (
    get_all_releases,
    extract_release_metadata,
)
from fetch_reviews import (
    get_all_reviews,
    extract_review_metadata,
)

logging.basicConfig(level=logging.CRITICAL)
logger = logging.getLogger("integration test")


def fetch_extensions_integration_test():
    """TODO"""

    num_total_extensions = get_total_number_of_extensions(logger)
    print(f"Total number of extensions in the marketplace: {num_total_extensions}")

    page_size = 100
    num_extension_pages = calculate_number_of_extension_pages(
        num_total_extensions, page_size
    )
    print(
        f"\nTotal number of pages of extensions to query with page size of {page_size}: "
        f"{num_extension_pages}"
    )

    extensions = get_all_extensions(logger, 1)
    print("\nThe first extension retrieved:")
    print(json.dumps(extensions[0], indent=4))

    extensions_df = extract_extension_metadata(extensions)
    print("\nThe first five rows of the extension dataframe:")
    print(extensions_df.head())

    publishers_df = extract_publisher_metadata(extensions)
    print("\nThe first five rows of the publisher dataframe:")
    print(publishers_df.head())

    extensions_df = clean_dataframe(extensions_df)
    print("\nThe first five rows of the cleaned extension dataframe:")
    print(extensions_df.head())

    publishers_df = clean_dataframe(publishers_df)
    print("\nThe first five rows of the cleaned publisher dataframe:")
    print(publishers_df.head())


def fetch_releases_integration_test():
    """TODO"""

    connection = connect_to_database(logger)

    extensions_df = select_extensions(logger, connection)
    print("The first five rows of the extensions_df:")
    print(extensions_df.head())

    releases_df = select_latest_releases(logger, connection)
    print("The first five rows of the releases_df:")
    print(releases_df.head())

    releases = get_all_releases(logger, extensions_df.head(), releases_df)
    print("\nThe first set of releases retrieved:")
    print(json.dumps(next(iter(releases.items())), indent=4))

    releases_df = extract_release_metadata(logger, releases)
    print("The first five rows of the releases_df:")
    print(releases_df.head())

    releases_df = clean_dataframe(releases_df)
    print("The first five rows of the cleaned releases_df:")
    print(releases_df.head())

    connection.close()


def fetch_reviews_integration_test():
    """TODO"""

    connection = connect_to_database(logger)

    extensions_df = select_extensions(logger, connection)
    print("The first five rows of the extensions_df:")
    print(extensions_df.head())

    publishers_df = select_publishers(logger, connection)
    print("The first five rows of the publishers_df:")
    print(publishers_df.head())

    extensions_publishers_df = combine_dataframes(
        [extensions_df, publishers_df], ["publisher_id"]
    )
    print("The first five rows of the extensions_publishers_df:")
    print(extensions_publishers_df.head())

    reviews = get_all_reviews(logger, extensions_publishers_df.head())
    print("\nThe first set of reviews retrieved:")
    print(json.dumps(reviews, indent=4))

    reviews_df = extract_review_metadata(reviews)
    print("The first five rows of the reviews_df:")
    print(reviews_df.head())

    reviews_df = clean_dataframe(reviews_df)
    print("The first five rows of the cleaned reviews_df:")
    print(reviews_df.head())

    connection.close()


def main() -> None:
    """TODO"""

    fetch_extensions_integration_test()
    fetch_releases_integration_test()
    fetch_reviews_integration_test()


if __name__ == "__main__":
    main()
