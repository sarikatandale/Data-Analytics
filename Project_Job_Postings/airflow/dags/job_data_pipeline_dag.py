# dags/job_data_pipeline_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
import logging
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Absolute import path (inside Airflow container)
from src.data_gen.generate_jobs import generate_job_data, save_to_csv
from src.loaders.load_to_snowflake import load_csv_to_snowflake

# ---------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------
logger = logging.getLogger("job_data_pipeline")

# ---------------------------------------------------------------------
# DAG Default Configuration
# ---------------------------------------------------------------------
default_args = {
    "owner": "data_eng",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

# ---------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------
with DAG(
    dag_id="job_data_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["jobs", "etl", "snowflake", "dbt"],
    description="End-to-end ETL pipeline: generate fake job data, load to Snowflake, run dbt transformations.",
) as dag:

    # -------------------------------------------------------------
    # Generate job posting data and save to CSV
    # -------------------------------------------------------------
    def generate_and_save():
        """Generate job postings and save to CSV."""
        output_path = "/opt/airflow/src/data/job_postings.csv"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        logger.info("Generating job data...")
        df = generate_job_data()
        save_to_csv(df, output_path)
        logger.info(f"Generated {len(df)} job records at {output_path}")

    generate_task = PythonOperator(
        task_id="generate_jobs",
        python_callable=generate_and_save,
    )

    # -------------------------------------------------------------
    # Load data into Snowflake
    # -------------------------------------------------------------
    load_task = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_csv_to_snowflake,
        op_args=["/opt/airflow/src/data/job_postings.csv"],
        op_kwargs={"table_name": "ANALYTICS.JOB_POSTING"},
    )

    # -------------------------------------------------------------
    # (Optional) Task 3: Run dbt transformations
    # -------------------------------------------------------------
    run_dbt = BashOperator(
        task_id="run_dbt_transformations",
        bash_command="cd /opt/airflow/dbt/Job_Postings && dbt run",
    )

    # -------------------------------------------------------------
    # Task Dependency Chain
    # -------------------------------------------------------------
    generate_task >> load_task >> run_dbt
