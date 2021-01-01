import os
import time
import stat
import math
from typing import List
from datetime import datetime
import uuid

# AWS Access
from botocore.client import BaseClient
import boto3


def main():
    stale_files = get_stale_files(".", days_threshold=30)
    upload_to_s3(file_paths=stale_files)


def days_stale(path: str):
    if os.path.isfile(path):
        return _days_stale_of_file(path)
    else:
        return _days_stale_of_folder(path)


def _days_stale_of_folder(folder_path: str):
    latest_days = None
    for file in _all_files_in_dir(folder_path):
        days = _days_stale_of_file(file)
        if latest_days is None or days < latest_days:
            latest_days = days

    return latest_days if latest_days is not None else 0


def _days_stale_of_file(file_path: str):
    file_stats_result = os.stat(file_path)
    access_time = file_stats_result[stat.ST_ATIME]
    seconds_stale = time.time() - access_time
    days_stale = math.floor(seconds_stale / 86400)
    return days_stale


def _all_files_in_dir(path: str):
    files = []
    for r, _, f in os.walk(path):
        for file in f:
            sub_path = os.path.join(r, file)
            files.append(sub_path)
    return files


def get_stale_files(root_path: str, days_threshold: int):

    stale_sub_paths = []
    for sub_path in os.listdir(root_path):
        path = os.path.join(root_path, sub_path)
        if days_stale(path) >= days_threshold:
            stale_sub_paths.append(path)

    stale_files = []
    for sub_path in stale_sub_paths:
        if os.path.isfile(sub_path):
            stale_files.append(sub_path)
        else:
            stale_files += _all_files_in_dir(sub_path)

    return stale_files


def upload_to_s3(file_paths: List[str], s3_client: BaseClient = None):
    if s3_client is None:
        s3_client = boto3.client("s3")

    try:
        bucket_name = _generate_unique_bucket_name()
        print(f"Creating bucket: {bucket_name}")
        s3_client.create_bucket(Bucket=bucket_name)

        for i, file_path in enumerate(file_paths):
            print(f"Uploading ({i + 1}/{len(file_paths)}): {file_path}")
            s3_client.upload_file(file_path, bucket_name, file_path)

    except Exception as e:
        print(f"ERROR: Failed uploading to S3: {e}")


def _generate_unique_bucket_name():
    current_date = datetime.now()
    date_str = current_date.strftime("%Y-%m-%d")
    short_uid = uuid.uuid4().hex[:12]
    bucket_name = f"archive.{date_str}.{short_uid}"
    return bucket_name


if __name__ == "__main__":
    main()
