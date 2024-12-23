"""
test_vscavator.py
"""

import unittest
from unittest.mock import MagicMock
import responses
import requests

from vscavator import (
    get_total_number_of_extensions,
    calculate_number_of_extension_pages,
    get_extensions,
    get_extension_releases,
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

        responses.add(
            responses.POST,
            extensions_url,
            json=mock_response,
            status=200,
        )

        # Call the function
        total_count = get_total_number_of_extensions(mock_logger)

        # Assertions
        self.assertEqual(total_count, 1500)
        mock_logger.info.assert_called_with(
            "get_total_number_of_extensions: total number of extensions is %d",
            1500,
        )

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
        mock_logger.critical.assert_called_with(
            "get_total_number_of_extensions: Error fetching number of extensions: "
            "status code %d",
            500,
        )


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

        # Mocking the logger and session
        mock_logger = MagicMock()
        mock_session = requests.Session()

        # Mock API response
        mock_extensions = [{"id": "ext1"}, {"id": "ext2"}]
        mock_response = {"results": [{"extensions": mock_extensions}]}

        responses.add(
            responses.POST,
            extensions_url,
            json=mock_response,
            status=200,
        )

        # Call the function
        page_number = 1
        extensions = get_extensions(mock_logger, mock_session, page_number)

        # Assertions
        self.assertEqual(extensions, mock_extensions)
        mock_logger.info.assert_called_with(
            "get_extensions: Fetched %d extensions from page number %d",
            len(mock_extensions),
            page_number,
        )

    @responses.activate
    def test_get_extensions_failure(self):
        """TODO"""

        extensions_url = (
            "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery"
        )

        # Mocking the logger and session
        mock_logger = MagicMock()
        mock_session = requests.Session()

        # Mock API failure response
        responses.add(
            responses.POST,
            extensions_url,
            status=500,
        )

        # Call the function
        page_number = 1
        extensions = get_extensions(mock_logger, mock_session, page_number)

        # Assertions
        self.assertEqual(extensions, [])
        mock_logger.critical.assert_called_with(
            "get_extensions: Error fetching extensions from page number %d: "
            "status code %d",
            page_number,
            500,
        )


class TestGetExtensionReleases(unittest.TestCase):
    """TODO"""

    @responses.activate
    def test_get_extension_releases_success(self):
        """TODO"""

        extensions_url = (
            "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery"
        )

        # Mock logger and session
        mock_logger = MagicMock()
        mock_session = requests.Session()

        # Mock API response
        extension_identifier = "publisher.extension"
        mock_response = {
            "results": [
                {
                    "extensions": [
                        {
                            "versions": [
                                {
                                    "version": "1.0.0",
                                    "lastUpdated": "2024-12-01T00:00:00Z",
                                },
                                {
                                    "version": "1.1.0",
                                    "lastUpdated": "2024-12-10T00:00:00Z",
                                },
                            ]
                        }
                    ]
                }
            ]
        }

        responses.add(
            responses.POST,
            extensions_url,
            json=mock_response,
            status=200,
        )

        # Call the function
        releases = get_extension_releases(
            logger=mock_logger,
            session=mock_session,
            extension_identifier=extension_identifier,
        )

        # Assertions
        self.assertEqual(len(releases), 2)
        self.assertEqual(releases[0]["version"], "1.0.0")
        self.assertEqual(releases[1]["version"], "1.1.0")
        mock_logger.info.assert_called_with(
            "get_extension_releases: Fetched extension releases for extension %s",
            extension_identifier,
        )

    @responses.activate
    def test_get_extension_releases_failure(self):
        """TODO"""

        extensions_url = (
            "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery"
        )

        # Mock logger and session
        mock_logger = MagicMock()
        mock_session = requests.Session()

        # Mock API failure response
        extension_identifier = "publisher.extension"
        responses.add(
            responses.POST,
            extensions_url,
            status=500,
        )

        # Call the function
        releases = get_extension_releases(
            logger=mock_logger,
            session=mock_session,
            extension_identifier=extension_identifier,
        )

        # Assertions
        self.assertEqual(releases, {})
        mock_logger.error.assert_called_with(
            "get_extension_releases: Error fetching releases for extension %s "
            "from page number %d: status code %d",
            extension_identifier,
            1,
            500,
        )


if __name__ == "__main__":
    unittest.main()
