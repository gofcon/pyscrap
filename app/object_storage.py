"""OCI Object Storage client (S3-compatible API), used by
:func:`app.services.export.upload_file` to put a staged file into the bucket.

What goes through here is what a table cannot hold -- a scrape whose result
is a document rather than rows. Row data reaches the same bucket without
touching this process: DBMS_CLOUD.EXPORT_DATA writes the Parquet from inside
the database (scripts/sql/sp_export_parquet.sql), reading the result table
directly, so an export covers rows that never passed through a run here and
can be repeated without depending on what happens to be staged on disk.

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
