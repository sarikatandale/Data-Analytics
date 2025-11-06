import pytest
import pandas as pd
from unittest.mock import patch
from src.loaders.load_to_snowflake import load_csv_to_snowflake

@patch("src.loaders.load_to_snowflake.get_snowflake_connection")
@patch("pandas.read_csv")
def test_load_csv_to_snowflake_mocks_read_and_connect(mock_read_csv, mock_get_conn):
    mock_df = pd.DataFrame({
        "job_id": ["abc"],
        "title": ["Engineer"],
        "company": ["TestCo"],
        "location": ["City"],
        "salary": [100000],
        "posted_date": ["2025-01-01"],
        "employment_type": ["Full-time"],
        "ingestion_timestamp": ["2023-01-01T00:00:00Z"]
    })
    mock_read_csv.return_value = mock_df
    mock_cursor = mock_get_conn.return_value.cursor.return_value
    load_csv_to_snowflake("fake/path/jobs.csv")
    assert mock_cursor.execute.called
