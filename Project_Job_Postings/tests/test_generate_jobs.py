import pandas as pd
from src.data_gen.generate_jobs import generate_fake_job_data

def test_generate_fake_job_data_structure():
    df = generate_fake_job_data(10)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 10
    assert "job_id" in df.columns
    assert "title" in df.columns
    assert "salary" in df.columns
