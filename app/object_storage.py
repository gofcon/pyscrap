"""OCI Object Storage client (S3-compatible API), used by
:mod:`app.services.export` to export typed-table scrape results as Parquet
-- so they can later be read via an Oracle external table pointed at the
bucket (one Parquet object per job, organized {table_name}/{job_id}.parquet,
so a wildcard file_uri_list picks up every job's file as one logical table
without merging files by hand).

boto3's newer default "flexible checksums" behavior streams the request body
with aws-chunked Content-Encoding, which OCI's S3-compat endpoint rejects
("AWS chunked encoding not supported"). Disabling checksum streaming via
Config is what makes plain put_object work against OCI.
"""

import os

import boto3
from botocore.config import Config


def get_client():
    namespace = os.environ["OCI_NAMESPACE"]
    region = os.environ["OCI_REGION"]
    access_key = os.environ["OCI_ACCESS_KEY"]
    secret_key = os.environ["OCI_SECRET_KEY"]

    endpoint = f"https://{namespace}.compat.objectstorage.{region}.oraclecloud.com"
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint,
        region_name=region,
        config=Config(
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            s3={"payload_signing_enabled": False},
        ),
    )


def bucket_name() -> str:
    return os.environ["OCI_BUCKET_NAME"]
