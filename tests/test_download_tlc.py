"""Tests for ingestion/download_tlc.py -- URL/S3-key construction and error
handling, mocked so no real network or AWS calls happen."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from ingestion.download_tlc import S3_BUCKET, TLC_BASE_URL, download_and_upload


@patch("ingestion.download_tlc.boto3.client")
@patch("ingestion.download_tlc.requests.get")
def test_download_and_upload_builds_correct_url_and_s3_key(mock_get, mock_boto_client):
    mock_response = MagicMock()
    mock_get.return_value = mock_response
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    result = download_and_upload(2026, 1)

    mock_get.assert_called_once_with(
        f"{TLC_BASE_URL}/yellow_tripdata_2026-01.parquet", stream=True, timeout=60
    )
    mock_response.raise_for_status.assert_called_once()
    mock_s3.upload_fileobj.assert_called_once_with(
        mock_response.raw,
        S3_BUCKET,
        "raw/taxi/year=2026/month=01/yellow_tripdata_2026-01.parquet",
    )
    assert result == (
        f"s3://{S3_BUCKET}/raw/taxi/year=2026/month=01/yellow_tripdata_2026-01.parquet"
    )


@patch("ingestion.download_tlc.boto3.client")
@patch("ingestion.download_tlc.requests.get")
def test_download_and_upload_zero_pads_single_digit_month(mock_get, mock_boto_client):
    mock_get.return_value = MagicMock()
    mock_boto_client.return_value = MagicMock()

    download_and_upload(2025, 9)

    called_url = mock_get.call_args[0][0]
    assert called_url == f"{TLC_BASE_URL}/yellow_tripdata_2025-09.parquet"


@patch("ingestion.download_tlc.boto3.client")
@patch("ingestion.download_tlc.requests.get")
def test_download_and_upload_raises_on_http_error(mock_get, mock_boto_client):
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
    mock_get.return_value = mock_response

    with pytest.raises(requests.HTTPError):
        download_and_upload(2099, 1)

    mock_boto_client.assert_not_called()
