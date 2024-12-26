"""
TODO
"""

import logging
import json
import os
import sys

sys.path.append(os.path.realpath("../.."))

# pylint: disable=C0413
from fetch_extensions import (
    get_total_number_of_extensions,
    calculate_number_of_extension_pages,
    get_all_extensions,
    extract_extension_metadata,
    extract_publisher_metadata,
)

# pylint: enable=C0413

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


def main() -> None:
    """TODO"""

    fetch_extensions_integration_test()


if __name__ == "__main__":
    main()
