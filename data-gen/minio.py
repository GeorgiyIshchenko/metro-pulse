from pathlib import Path
import boto3
from tqdm.auto import tqdm


def push_to_minio(data_dir: Path, endpoint: str, bucket: str, auth: tuple[str, str]):
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=auth[0],
        aws_secret_access_key=auth[1],
    )

    existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)

    files = data_dir.glob("*.parquet")
    for path in tqdm(files, desc="Uploading to Minio", unit=" files"):
        key = f"{path.stem}"
        print(f"Uploading {path} -> s3://{bucket}/{key}")
        s3.upload_file(str(path), bucket, key)
