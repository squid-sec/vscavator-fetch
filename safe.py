"""
safe.py helps ensure data accesses are safe
"""

from logging import Logger
from typing import Any, Optional
import requests

def validate_type(
    logger: Logger,
    value: Any,
    expected_type: type,
    context: str
) -> bool:
    """
    validate_type checks the type of the value matches what is expected
    """

    if not isinstance(value, expected_type):
        logger.error(
            "Expected %s in %s, got %s",
            expected_type, context, type(value)
        )
        return False
    return True

def get_value_from_dict(
    logger: Logger,
    data: dict,
    key: str,
    context: str
) -> Optional[Any]:
    """
    get_value_from_dict attempts to extract the value of the key from the dict
    """

    value = data.get(key)
    if value is None:
        logger.error(
            "Missing '%s' in %s",
            key, context
        )
    return value

def convert_to_json(
    logger: Logger,
    response: requests.Response
) -> Optional[dict]:
    """
    convert_to_json safely converts the response JSON to a dictionary
    """

    try:
        return response.json()
    except ValueError as e:
        logger.error("Failed to decode JSON response: %s", str(e))
        return None
