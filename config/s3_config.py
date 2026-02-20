import os

S3_CONFIG = {
    "bucket": os.getenv("S3_BUCKET"),
    "region": os.getenv("AWS_REGION"),
}

