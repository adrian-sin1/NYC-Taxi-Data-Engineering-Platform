"""Download a month of NYC TLC yellow taxi trip data and upload it to S3."""

import argparse
import sys

import boto3
import requests

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
S3_BUCKET = "nyc-transportation-adrian"


def download_and_upload(year: int, month: int) -> str:
    filename = f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
    url = f"{TLC_BASE_URL}/{filename}"
    s3_key = f"raw/taxi/year={year:04d}/month={month:02d}/{filename}"

    print(f"Downloading {url}")
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    s3 = boto3.client("s3")
    print(f"Uploading to s3://{S3_BUCKET}/{s3_key}")
    s3.upload_fileobj(response.raw, S3_BUCKET, s3_key)

    return f"s3://{S3_BUCKET}/{s3_key}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True, choices=range(1, 13))
    args = parser.parse_args()

    try:
        s3_path = download_and_upload(args.year, args.month)
    except requests.HTTPError as e:
        print(f"Failed to download TLC data: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Done: {s3_path}")
