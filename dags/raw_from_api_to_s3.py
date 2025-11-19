import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Конфигурация DAG
OWNER = "mylenalll"
DAG_ID = "raw_from_api_to_s3"

# Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "earthquake"

# S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 11, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

def get_dates(**context) -> tuple[str, str]:
    """
    Возвращает start_date и end_date в формате YYYY-MM-DD.
    Если переданы вручную start_date и end_date, использует их,
    иначе берёт из контекста Airflow.
    """
    start_date = context.get("manual_start_date")
    end_date = context.get("manual_end_date")

    if start_date is None:
        start_date = context["data_interval_start"].format("YYYY-MM-DD")
    if end_date is None:
        end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date


def get_and_transfer_api_data_to_s3(**context):

    start_date_str, end_date_str = get_dates(**context)

    # Convert to datetime objects
    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d")

    logging.info(f"💻 Start load for dates from {start_date_str} to {end_date_str}")

    con = duckdb.connect()

    con.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;
    """,
    )

    # Loop through each day
    current_dt = start_dt
    while current_dt <= end_dt:
        day = current_dt.strftime("%Y-%m-%d")

        con.sql(
            f"""
            COPY
            (
                SELECT
                    *
                FROM
                    read_csv_auto("https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={day}&endtime={day}") AS res
            ) TO 's3://prod/{LAYER}/{SOURCE}/{day}/{day}_00-00-00.gz.parquet';

            """,
        )

        logging.info(f"✅ Successfully loaded: {day}")

        current_dt += timedelta(days=1)
    con.close()
    logging.info(f"🎉 Finished loading all dates from {start_date_str} to {end_date_str}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "raw"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    get_and_transfer_api_data_to_s3 = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
        # Раскомментировать чтобы передать даты вручную
        # op_kwargs={
        #     "manual_start_date": "2025-11-01",
        #     "manual_end_date": "2025-11-03",
        # },
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> get_and_transfer_api_data_to_s3 >> end