FROM apache/airflow:2.7.1-python3.11

USER airflow

COPY src /opt/airflow/src
COPY dbt /opt/airflow/dbt
COPY airflow/dags /opt/airflow/dags
COPY tests /opt/airflow/tests

RUN pip install --no-cache-dir \
                    pandas \
                    snowflake-connector-python \
                    dbt-core \
                    dbt-snowflake \
                    python-dotenv \
                    faker
USER airflow
