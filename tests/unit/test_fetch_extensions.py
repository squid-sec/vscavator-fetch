"""
test_vscavator.py
"""

import unittest
from unittest.mock import MagicMock
import responses
import os
import sys

sys.path.append(os.path.realpath("../.."))

# pylint: disable=C0413

from util import add_mock_response
from fetch_extensions import (
    get_total_number_of_extensions,
    calculate_number_of_extension_pages,
    get_extensions,
    extract_extension_statistics,
    extract_extension_github_url,
)


class TestGetTotalNumberOfExtensions(unittest.TestCase):
    """TODO"""

    @responses.activate
    def test_get_total_number_of_extensions_success(self):
        """TODO"""

        extensions_url = (
            "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery"
        )

        # Mock logger
        mock_logger = MagicMock()

        # Mock API response
        mock_response = {
            "results": [
                {
                    "resultMetadata": [
                        {
                            "metadataType": "ResultCount",
                            "metadataItems": [{"name": "TotalCount", "count": 1500}],
                        }
                    ]
                }
            ]
        }

        add_mock_response(extensions_url, mock_response, 200)

        # Call the function
        total_count = get_total_number_of_extensions(mock_logger)

        # Assertions
        self.assertEqual(total_count, 1500)

    @responses.activate
    def test_get_total_number_of_extensions_failure(self):
        """TODO"""

        extensions_url = (
            "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery"
        )

        # Mock logger
        mock_logger = MagicMock()

        # Mock API failure response
        responses.add(
            responses.POST,
            extensions_url,
            status=500,
        )

        # Call the function
        total_count = get_total_number_of_extensions(mock_logger)

        # Assertions
        self.assertEqual(total_count, 0)


class TestCalculateNumberOfExtensionPages(unittest.TestCase):
    """TODO"""

    def test_zero_extensions(self):
        """Case where num_extensions is zero"""
        self.assertEqual(calculate_number_of_extension_pages(0), 1)

    def test_divisible_by_page_size(self):
        """Case where num_extensions is exactly divisible by page size"""
        self.assertEqual(calculate_number_of_extension_pages(100), 2)

    def test_less_than_page_size(self):
        """Case where num_extensions is less than page size"""
        self.assertEqual(calculate_number_of_extension_pages(10), 1)

    def test_greater_than_page_size(self):
        """Case where num_extensions is greater than page size"""
        self.assertEqual(calculate_number_of_extension_pages(110), 2)

    def test_many_extensions(self):
        """Case where num_extensions is very high"""
        self.assertEqual(calculate_number_of_extension_pages(50000), 501)


class TestGetExtensions(unittest.TestCase):
    """TODO"""

    @responses.activate
    def test_get_extensions_success(self):
        """TODO"""

        extensions_url = (
            "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery"
        )

        # Mocking the logger
        mock_logger = MagicMock()

        # Mock API response
        mock_extensions = [{"id": "ext1"}, {"id": "ext2"}]
        mock_response = {"results": [{"extensions": mock_extensions}]}

        add_mock_response(extensions_url, mock_response, 200)

        # Call the function
        page_number = 1
        extensions = get_extensions(mock_logger, page_number)

        # Assertions
        self.assertEqual(extensions, mock_extensions)

    @responses.activate
    def test_get_extensions_failure(self):
        """TODO"""

        extensions_url = (
            "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery"
        )

        # Mocking the logger
        mock_logger = MagicMock()

        # Mock API failure response
        responses.add(
            responses.POST,
            extensions_url,
            status=500,
        )

        # Call the function
        page_number = 1
        extensions = get_extensions(mock_logger, page_number)

        # Assertions
        self.assertEqual(extensions, [])


class TestExtractExtensionStatistics(unittest.TestCase):
    """TODO"""

    def test_extract_extension_statistics(self):
        """TODO"""
        input_statistics = [
            {"statisticName": "install", "value": 1500},
            {"statisticName": "averagerating", "value": 4.5},
            {"statisticName": "ratingcount", "value": 200},
            {"statisticName": "trendingdaily", "value": 50},
            {"statisticName": "trendingmonthly", "value": 100},
            {"statisticName": "trendingweekly", "value": 70},
            {"statisticName": "updateCount", "value": 10},
            {"statisticName": "weightedRating", "value": 4.7},
            {"statisticName": "downloadCount", "value": 5000},
            {"statisticName": "irrelevantStat", "value": 9999},
        ]

        expected_output = {
            "install": 1500,
            "averagerating": 4.5,
            "ratingcount": 200,
            "trendingdaily": 50,
            "trendingmonthly": 100,
            "trendingweekly": 70,
            "updateCount": 10,
            "weightedRating": 4.7,
            "downloadCount": 5000,
        }

        actual_output = extract_extension_statistics(input_statistics)

        self.assertEqual(actual_output, expected_output)


class TestExtractExtensionGithubURL(unittest.TestCase):
    """TODO"""

    def test_extract_extension_github_url_success(self):
        """TODO"""
        properties = [
            {
                "key": "Microsoft.VisualStudio.Services.Links.GitHub",
                "value": "https://github.com/example/repo",
            },
            {"key": "Other.Property", "value": "Some Value"},
        ]

        expected_output = "https://github.com/example/repo"
        actual_output = extract_extension_github_url(properties)
        self.assertEqual(actual_output, expected_output)

    def test_extract_extension_github_url_no_github_url(self):
        """TODO"""
        properties = [
            {"key": "Some.Other.Key", "value": "https://other-link.com"},
            {"key": "Other.Property", "value": "Some Value"},
        ]

        expected_output = ""
        actual_output = extract_extension_github_url(properties)
        self.assertEqual(actual_output, expected_output)

    def test_extract_extension_github_url_empty_properties(self):
        """TODO"""
        properties = []

        expected_output = ""
        actual_output = extract_extension_github_url(properties)
        self.assertEqual(actual_output, expected_output)


if __name__ == "__main__":
    unittest.main()
