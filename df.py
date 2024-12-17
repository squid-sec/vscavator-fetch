"""
df.py contains dataframe related functions
"""

import pandas as pd

def combine_dataframes(
    extensions_df: pd.DataFrame,
    publishers_df: pd.DataFrame,
    releases_df: pd.DataFrame
) -> pd.DataFrame:
    """
    combine_dataframes merges the extensions, publishers, and releases dataframes
    """

    releases_extensions_df = releases_df.merge(extensions_df, on="extension_id", how="inner")
    combined_df = releases_extensions_df.merge(publishers_df, on="publisher_id", how="inner")
    return combined_df

def object_keys_to_dataframe(
    object_keys: list
) -> pd.DataFrame:
    """
    object_keys_to_dataframe extracts the publisher name, extension name, and extension
    version from the S3 object keys
    """

    parsed_object_keys = []

    for object_key in object_keys:
        fields = object_key.split("/")
        parsed_object_keys.append({
            "publisher_name": fields[1],
            "extension_name": fields[2],
            "version": fields[3].replace(".vsix", "")
        })

    return pd.DataFrame(parsed_object_keys)

def verified_uploaded_to_s3(
    df: pd.DataFrame,
    publisher_name: str,
    extension_name: str,
    version: str
) -> bool:
    """
    verified_uploaded_to_s3 checks if the status of given extension release in the
    dataframe is uploaded to S3
    """

    return not df.loc[
        (df["publisher_name"] == publisher_name) &
        (df["extension_name"] == extension_name) &
        (df["version"] == version)
    ].empty
