import os
import shutil
from unittest.mock import Mock

from archive import main, get_stale_files, days_stale, upload_to_s3
from file_generator import generate_test_set, generate_directory, generate_test_file


TMP_PATH = "tmp"


def setup():
    os.makedirs(TMP_PATH, exist_ok=True)


def teardown():
    shutil.rmtree(TMP_PATH)


def test_days_stale():

    directory_path = generate_directory(TMP_PATH, "my_test_directory")
    new_file = generate_test_file(directory_path, days_old=0)
    stale_file = generate_test_file(directory_path, days_old=360)

    assert days_stale(new_file) == 0
    assert days_stale(stale_file) == 360
    assert days_stale(directory_path) == 0


def test_get_stale_files():
    generate_test_set(TMP_PATH)
    stale_files = get_stale_files(TMP_PATH, days_threshold=30)

    for f in stale_files:
        print(f"Found stale file: {f}")

    # In the test set, there should be exactly 7 stale files older than 30 days.
    assert len(stale_files) == 7


def test_upload_to_s3():
    generate_test_set(TMP_PATH)
    stale_files = get_stale_files(TMP_PATH, days_threshold=30)

    mock_s3_client = Mock()
    mock_s3_client.create_bucket = Mock()
    mock_s3_client.upload_file = Mock()

    upload_to_s3(stale_files, mock_s3_client)

    assert mock_s3_client.create_bucket.called
    assert mock_s3_client.upload_file.call_count == 7
