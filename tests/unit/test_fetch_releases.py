"""
TODO
"""

import unittest
from unittest.mock import MagicMock
import responses

from fetch_releases import get_extension_releases


class TestGetExtensionReleases(unittest.TestCase):
    """TODO"""

    @responses.activate
    def test_get_extension_releases_success(self):
        """TODO"""

        extensions_url = (
            "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery"
        )

        # Mock logger
        mock_logger = MagicMock()

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
            extension_identifier=extension_identifier,
        )

        # Assertions
        self.assertEqual(len(releases), 2)
        self.assertEqual(releases[0]["version"], "1.0.0")
        self.assertEqual(releases[1]["version"], "1.1.0")

    @responses.activate
    def test_get_extension_releases_failure(self):
        """TODO"""

        extensions_url = (
            "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery"
        )

        # Mock logger
        mock_logger = MagicMock()

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
            extension_identifier=extension_identifier,
        )

        # Assertions
        self.assertEqual(releases, {})


if __name__ == "__main__":
    unittest.main()
