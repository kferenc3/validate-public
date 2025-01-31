'''Functions related to S3 read/write'''
import os
import logging

from datetime import datetime
from urllib.parse import urlparse
from typing import Tuple

import boto3
import s3fs
import polars as pl

# ENV VARS
AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")
# Constants for process results

PRESIGNED_URL_DURATION = 60 * 60

# S3 Client
S3_CLIENT = boto3.client("s3")

# Logger configuration
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s ")
LOGGER = logging.getLogger("aws_helpers")
LOGGER.setLevel(os.getenv("LOGLEVEL", "DEBUG"))

def parse_s3_path(path: str) -> Tuple[str, str]:
    """
    S3 path parser
    :param path: S3 path to parse (s3://bucket/path/to/file.csv)
    :return: Bucket name, Key
    """
    parts = urlparse(path)
    LOGGER.info("Bucket: %s - Key: %s", parts.netloc, parts.path.lstrip("/"))
    return parts.netloc, parts.path.lstrip("/")

def put_results_s3(data: pl.DataFrame, src_path: str, tgt_path) -> bool:
    """
    Writes a CSV file to an Amazon S3 bucket
    :param data: pl.Dataframe
    :param task_config: dict
    :return: True if src_data was added to s3_bucket, otherwise False
    """
    LOGGER.info("Loading result CSV file to S3...")

    try:
        execution_starttime = datetime.now()
        log_path = (
            "ValidationResult")
        source_file = parse_s3_path(src_path)[1]
        s3_key_log = (
            f'{log_path}'
            f'{execution_starttime.strftime("/%Y/%m/%d/")}'
            f'{source_file[:source_file.find('.')]}_result.csv'
        )
        s3_bucket_log = parse_s3_path(tgt_path)[0]
        LOGGER.debug("Results to be stored in: s3://%s/%s", s3_bucket_log, s3_key_log)

        fs = s3fs.S3FileSystem()
        destination = f's3://{s3_bucket_log}/{s3_key_log}'
        with fs.open(destination, 'wb') as f:
            data.write_csv(f)
            LOGGER.info("Result CSV file successfully uploaded on %s", destination)
        return True
    except Exception as e:
        LOGGER.error(e)
        return False

def generate_storage_creds() -> dict:
    '''
    The function assumes a profile is set in the environment.
    It returns the credentials for the profile
    '''
    session = boto3.session.Session()
    credentials = session.get_credentials().get_frozen_credentials()
    return {
        "aws_access_key_id": credentials.access_key,
        "aws_secret_access_key": credentials.secret_key,
        "aws_region": AWS_REGION,
    }
