
"""
Snowflake Data loader

This script includes functions to:
1. Connect to Snowflake.
2. Create and populate Job_Posting table with data from a Dataframe.

"""

import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import logging
from typing import Optional
# -------------------------------------------------------------------
# Load environment variables from .env
# -------------------------------------------------------------------
load_dotenv()

# -------------------------------------------------------------------
# Logging Configuration
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("Snowflake_loader_logger")
logger.info("Starting Snowflake loaders")


# --------------------------------------------------------------------------------------------
# Functions to establish connection to Snowflake and insert data in Snowflake table
# --------------------------------------------------------------------------------------------

def get_snowflake_connection():
    """Establish connection to Snowflake"""
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )
        logger.info("Successfully connected to Snowflake.")
        return conn
    except snowflake.connector.errors.Error as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise

# -------------------------------------------------------------------
# Insert dataframe into Snowflake
# -------------------------------------------------------------------
def insert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    connection: Optional[snowflake.connector.SnowflakeConnection] = None
) -> None:
    """
    Insert a DataFrame into a table created in Snowflake

    Parameters
    ----------------
    df: pd.DataFrame
        The DataFrame consisting of job records
    table_name: str
         Name of the Snowflake table
    connection: snowflake.connector.SnowflakeConnection
         Snowflake Connection
    """
    if df.empty:
        logger.warning("DataFrame is empty..")
        return

    close_conn = False
    if connection is None:
        connection = get_snowflake_connection()
        close_conn = True

    cursor = connection.cursor()

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "job_posting"),
        database=os.getenv("SNOWFLAKE_DATABASE", "JOB_POSTING_DATA"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
    )
   
    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                JOB_ID STRING,
                TITLE STRING,
                COMPANY STRING,
                LOCATION STRING,
                SALARY INT,
                POSTED_DATE DATE,
                EMPLOYMENT_TYPE STRING,
                CREATE_DATE DATETIME
            )
        """)
        logger.info(f"Created table {table_name}")

        """Insert a DataFrame with Job records into a Snowflake table"""

        df.columns = [col.upper() for col in df.columns]
        print(df.columns)
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name.split('.')[-1])
        logger.info(f"Inserting {len(df)} rows into table Job_Posting...")
        
        
       
      #  connection.commit()
      # logger.info("Data inserted successfully in Job_Posting table.")

    except Exception as e:
        logger.error(f"Failed to insert data into Snowflake: {e}")
        raise
    finally:
        cursor.close()
        if close_conn:
            connection.close()
            logger.info("Snowflake connection is closed.")

# -------------------------------------------------------------------
# CSV File Loader
# -------------------------------------------------------------------
def load_csv_to_snowflake(file_path: str, table_name: str = "PUBLIC.JOB_POSTINGS") -> None:
    """
        Function to load a CSV into Snowflake.
       
        Parameters
        -----------
        file_path: str
            Path to the CSV file containing job records
        table_name: str
            Name of the table where data is inserted
    """
    try:
        logger.info(f"Reading CSV File from path: {file_path}")
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} records from CSV File.")
        insert_dataframe(df, table_name)
    except Exception as e:
        logger.error(f"Failed to load contents of CSV File into Snowflake: {e}")
        raise
