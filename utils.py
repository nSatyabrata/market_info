import boto3
from typing import Dict
from io import StringIO
from datetime import datetime
from config import aws_s3_config

class S3OperationError(Exception):
    """Custom error that is raised if there is any issue while operating with AWS s3."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


def upload_logs_to_s3(log_buffer: StringIO) -> None:

    bucket_name = 'economydataproject'
    today = datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
    s3_file_name = f"logs/{today}.log"
    try:
        s3 = boto3.client(**aws_s3_config)
        s3.put_object(Bucket=bucket_name, Key=s3_file_name, Body=log_buffer.getvalue())
    except Exception as error:
        raise S3OperationError(f"Upload failed due to {error=}")
