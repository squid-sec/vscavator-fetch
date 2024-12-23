"""
df.py contains dataframe related functions
"""

from typing import List
import pandas as pd


def combine_dataframes(
    dfs: List[pd.DataFrame], keys: List[str], how: str = "inner"
) -> pd.DataFrame:
    """
    combine_dataframes is a generic function to merge multiple dataframes based on specified keys
    """

    if len(dfs) - 1 != len(keys):
        raise ValueError(
            "Number of keys must be one less than the number of dataframes."
        )

    combined_df = dfs[0]
    for i, key in enumerate(keys):
        combined_df = combined_df.merge(dfs[i + 1], on=key, how=how)

    return combined_df


def object_keys_to_dataframe(object_keys: list) -> pd.DataFrame:
    """
    object_keys_to_dataframe extracts the publisher name, extension name, and extension
    version from the S3 object keys
    """

    parsed_object_keys = []

    for object_key in object_keys:
        fields = object_key.split("/")
        parsed_object_keys.append(
            {
                "publisher_name": fields[1],
                "extension_name": fields[2],
                "version": fields[3].replace(".vsix", ""),
            }
        )

    return pd.DataFrame(parsed_object_keys)


def verified_uploaded_to_s3(
    object_keys_df: pd.DataFrame, publisher_name: str, extension_name: str, version: str
) -> bool:
    """
    verified_uploaded_to_s3 checks if the status of given extension release in the
    dataframe is uploaded to S3
    """

    return not object_keys_df.loc[
        (object_keys_df["publisher_name"] == publisher_name)
        & (object_keys_df["extension_name"] == extension_name)
        & (object_keys_df["version"] == version)
    ].empty
