import json
import boto3
from botocore.exceptions import ClientError
from config.s3_config import S3_CONFIG


def get_s3_client():
    return boto3.client("s3", region_name=S3_CONFIG["region"])


def ensure_bucket_exists():
    s3 = get_s3_client()
    bucket = S3_CONFIG["bucket"]

    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        print(f"Creating bucket {bucket}")
        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={
                "LocationConstraint": S3_CONFIG["region"]
            }
        )


def get_watermark(table_name):
    s3 = get_s3_client()
    key = f"metadata/watermarks/{table_name}.json"

    try:
        obj = s3.get_object(Bucket=S3_CONFIG["bucket"], Key=key)
        content = json.loads(obj["Body"].read())
        return content["last_ingested_at"]
    except:
        return None


def set_watermark(table_name, value):
    s3 = get_s3_client()
    key = f"metadata/watermarks/{table_name}.json"

    s3.put_object(
        Bucket=S3_CONFIG["bucket"],
        Key=key,
        Body=json.dumps({"last_ingested_at": value})
    )
