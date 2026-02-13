import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import numpy as np  # Required for checking NaN/NaT
from io import BytesIO
from botocore.exceptions import ClientError
from config.minio_config import MINIO_CONFIG


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_CONFIG["endpoint_url"],
        aws_access_key_id=MINIO_CONFIG["aws_access_key_id"],
        aws_secret_access_key=MINIO_CONFIG["aws_secret_access_key"],
    )


def sanitize_df_for_spark(df):
    """
    Cleans Pandas DataFrame to ensure the resulting Parquet file
    is 100% compatible with Spark.
    """
    # Create a copy to avoid SettingWithCopy warnings
    df = df.copy()

    for col in df.columns:
        dtype = df[col].dtype

        # 1. Handle Timestamp / Datetime columns
        if pd.api.types.is_datetime64_any_dtype(dtype):
            # Convert NaT to None (Spark hates NaT)
            df[col] = df[col].replace({pd.NaT: None})

        # 2. Handle Object columns (Strings, Lists, Dicts, Mixed)
        elif dtype == 'object':
            # Convert complex objects (dicts/lists) to string
            # and generic objects to string.
            df[col] = df[col].astype(str)

            # 3. Clean up the "Stringified" Nulls
            # astype(str) turns None into 'None' and nan into 'nan'.
            # We must revert them to real None so Spark sees them as NULL.
            df[col] = df[col].replace({'None': None, 'nan': None, 'NaT': None, '<NA>': None})

    return df


def upload_parquet_to_s3(df, bucket, key):
    s3 = get_s3_client()

    # 1. Ensure Bucket Exists
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        print(f"⚠️ Bucket '{bucket}' not found. Creating it now...")
        try:
            s3.create_bucket(Bucket=bucket)
        except Exception as e:
            print(f"❌ Failed to create bucket '{bucket}': {e}")
            raise e

    # ---------------------------------------------------------
    # ✅ FIX: Sanitize Data for Spark
    # ---------------------------------------------------------
    print(f"🧹 Sanitizing {len(df)} rows for Spark compatibility...")
    try:
        clean_df = sanitize_df_for_spark(df)
    except Exception as e:
        print(f"❌ Sanitization failed: {e}")
        raise e

    # ---------------------------------------------------------
    # Write to Parquet and Upload
    # ---------------------------------------------------------
    try:
        # We explicitly use version='1.0' for maximum compatibility
        table = pa.Table.from_pandas(clean_df)
        buffer = BytesIO()
        pq.write_table(table, buffer, compression="snappy", version='1.0')
        buffer.seek(0)

        s3.upload_fileobj(buffer, bucket, key)
        print(f"🪣 Uploaded → s3://{bucket}/{key}")

    except Exception as e:
        print(f"❌ Parquet Conversion Failed: {e}")
        print("Dtypes that failed:")
        print(clean_df.dtypes)
        raise e