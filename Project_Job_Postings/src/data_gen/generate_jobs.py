"""
Data generator for job postings.
-------------------------------------------
Generates job posting data and saves it to CSV.
"""

import os
import logging
import random
from datetime import datetime
from typing import List
import pandas as pd
from faker import Faker

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
fake = Faker()
Faker.seed(42)

JOB_TITLES: List[str] = [
    "Data Engineer",
    "Analytics Engineer",
    "BI Developer",
    "SQL Developer",
    "Database Engineer",
]

EMPLOYMENT_TYPES: List[str] = ["Full-time", "Part-time", "Contract"]

# -------------------------------------------------------------------
# Logging Configuration
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("generate_jobs_logger")
logger.info("Starting job data generator...")

# -------------------------------------------------------------------
#  Functions to generate and save job data
# -------------------------------------------------------------------
def generate_job_data(n: int = 5000) -> pd.DataFrame:
    """
    Creates dataset of job posting records.

    Args:
        n (int): Number of job postings to simulate.

    Returns:
        pd.DataFrame: DataFrame containing generated job records.
    """
    logger.info("Generating %d job records...", n)

    records = []
    for i in range(n):
        record = {
            "job_id": i + 1,
            "title": random.choice(JOB_TITLES),
            "company": fake.company(),
            "location": fake.state(),
            "salary": random.randint(90000, 200000),
            "posted_date": fake.date_between(start_date="-30d", end_date="today"),
            "employment_type": random.choice(EMPLOYMENT_TYPES),
            "create_date": datetime.utcnow(),
        }
        records.append(record)

    df = pd.DataFrame(records)
    logger.info("Generated %d job records successfully.", len(df))
    df["posted_date"] = pd.to_datetime(df["posted_date"]).dt.date
    df["create_date"] = pd.to_datetime(df["create_date"])

    return df



def save_to_csv(df: pd.DataFrame, path: str) -> None:
    """
    Save DataFrame to CSV file.

    Args:
        df (pd.DataFrame): DataFrame to export to csv file
        path (str): Destination CSV path.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logger.info("Data saved successfully at: %s", path)


# -------------------------------------------------------------------
# Script Entry Point
# -------------------------------------------------------------------
if __name__ == "__main__":
    OUTPUT_PATH = "data/generated/job_postings.csv"

    df_jobs = generate_job_data(n=5000)
    save_to_csv(df_jobs, OUTPUT_PATH)
    logger.info("Job data generation completed.")
