import re
from typing import List, Tuple, Dict, Optional

import boto3
from boto3 import Session

S3_RESPONSE_COMMON_PREFIXES = "CommonPrefixes"
S3_RESPONSE_PREFIX = 'Prefix'
S3_RESPONSE_CONTENTS = 'Contents'
S3_RESPONSE_KEY = 'Key'
SLASH = "/"


def _get_credentials(aws_profile_name: str = "default") -> Dict:
    session = boto3.session.Session(profile_name=aws_profile_name)

    credentials = {
        "key": session.get_credentials().access_key,
        "secret": session.get_credentials().secret_key,
        "token": session.get_credentials().token
    }
    return credentials


def _get_s3_connection_object():
    creds = _get_credentials()

    session = Session(
        aws_access_key_id=creds["key"],
        aws_secret_access_key=creds["secret"],
        aws_session_token=creds["token"],
    )
    return session.client("s3")


def _extract_s3_bucket_prefix(path: str) -> Optional[Tuple[str, str]]:
    pattern = r"s3.*//([\w-]+)/(.*\/?)"
    if re.match(pattern, path):
        return re.compile(pattern).findall(path)[0]
    else:
        return None


def _validate_partitions_path(s3_path: str, partitions: Dict[str, str]) -> str:
    initial_path = s3_path.strip("/")
    tmp_partitions = partitions.copy()
    while len(tmp_partitions) > 0:
        for partition, value in list(tmp_partitions.items()):
            new_path = f"{initial_path}/{partition}={value}"
            if _is_valid_directory(new_path):
                initial_path = new_path.strip("/")
                tmp_partitions.pop(partition, None)
                break
        else:
            return False, None
    return True, f"{initial_path}/"


def _get_s3_list_response(s3_path):
    s3_connection = _get_s3_connection_object()
    bucket_name, prefix = _extract_s3_bucket_prefix(
        s3_path
    )
    return s3_connection.list_objects_v2(Bucket=bucket_name,
                                         Prefix=prefix, Delimiter=SLASH, MaxKeys=10)


def _is_valid_directory(s3_path) -> bool:
    s3_list_response = _get_s3_list_response(s3_path)
    if S3_RESPONSE_COMMON_PREFIXES not in s3_list_response:
        return False
    if S3_RESPONSE_PREFIX not in s3_list_response[S3_RESPONSE_COMMON_PREFIXES][0]:
        return False
    return True


def _is_valid_parquet_dir(s3_path: str) -> (bool, str):
    s3_list_response = _get_s3_list_response(s3_path)
    if S3_RESPONSE_CONTENTS not in s3_list_response:
        return False, None
    contents = s3_list_response[S3_RESPONSE_CONTENTS]
    parquet_files = [entry[S3_RESPONSE_KEY] for entry in contents if "parq" in entry[S3_RESPONSE_KEY]]
    return len(parquet_files) >= 1, f"{s3_path}*parquet"


def validate_path(s3_path: str, partitions: Dict[str, str]) -> bool:
    """
    Validates the existence of bucket, path and partitions of an S3 path

    Args:
        s3_path: An existing example of a partial S3 path (prefix before partitions)
        partitions: the partition names/values of the S3 path that we need to validate

    Returns: An S3ValidationStatus containing the result of whether the path is valid
    (existing and accesible by the user) and a concrete s3 path

    """
    # Case 2: User passes a partial S3 prefix for a partitioned dataset,
    # e.g: s3://datalake/pricing-triage/feature-data/tag=prod/
    # We need to derive paths based on the list of partitions the user has passed in
    if _is_valid_directory(s3_path):
        valid, full_path = _validate_partitions_path(s3_path, partitions)
        if valid:
            return _is_valid_parquet_dir(full_path)
        else:
            return False
    else:
        return False


if __name__ == "__main__":
    result = validate_path("s3://datalake-zillowgroup/zillow/lead-events-raw/raw/", {"data_date": "2020-09-06",
                                                                                     "file_date": "2020-09-15"})
    print(f"Result {result}")
