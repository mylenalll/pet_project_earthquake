import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow.sensors.python import PythonSensor
from airflow.models import DagRun
from airflow.utils.state import DagRunState
from airflow.utils.session import provide_session
from datetime import datetime

# Конфигурация DAG
OWNER = "mylenalll"
DAG_ID = "raw_from_s3_to_pg"

# Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"

# S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

# DuckDB
PASSWORD = Variable.get("pg_password")

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


@provide_session
def was_raw_dag_success_today(session=None, **kwargs):
    today = datetime.utcnow().date()

    runs = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == "raw_from_api_to_s3",
            DagRun.state == DagRunState.SUCCESS,
        )
        .all()
    )

    return any(run.execution_date.date() == today for run in runs)

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


from datetime import datetime, timedelta

def get_and_transfer_raw_data_to_ods_pg(**context):
    start_date_str, end_date_str = get_dates(**context)

    # Convert to datetime objects
    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d")

    logging.info(f"💻 Start load for dates from {start_date_str} to {end_date_str}")

    con = duckdb.connect()

    # Configure DuckDB once
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

        CREATE SECRET dwh_postgres (
            TYPE postgres,
            HOST 'postgres_dwh',
            PORT 5432,
            DATABASE postgres,
            USER 'postgres',
            PASSWORD '{PASSWORD}'
        );

        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);
        """
    )

    # Loop through each day
    current_dt = start_dt
    while current_dt <= end_dt:
        day = current_dt.strftime("%Y-%m-%d")
        parquet_path = f"s3://prod/{LAYER}/{SOURCE}/{day}/{day}_00-00-00.gz.parquet"

        logging.info(f"📥 Loading {parquet_path}")

        con.sql(
            f"""
            DELETE FROM dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
            WHERE time::date = '{day}'
            ;
            """
        )

        con.sql(
            f"""
            INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
            (
                time,
                latitude,
                longitude,
                depth,
                mag,
                mag_type,
                nst,
                gap,
                dmin,
                rms,
                net,
                id,
                updated,
                place,
                type,
                horizontal_error,
                depth_error,
                mag_error,
                mag_nst,
                status,
                location_source,
                mag_source
            )
            SELECT
                time,
                latitude,
                longitude,
                depth,
                mag,
                magType AS mag_type,
                nst,
                gap,
                dmin,
                rms,
                net,
                id,
                updated,
                place,
                type,
                horizontalError AS horizontal_error,
                depthError AS depth_error,
                magError AS mag_error,
                magNst AS mag_nst,
                status,
                locationSource AS location_source,
                magSource AS mag_source
            FROM '{parquet_path}';
            """
        )

        logging.info(f"✅ Successfully loaded: {day}")

        current_dt += timedelta(days=1)

    con.close()
    logging.info(f"🎉 Finished loading all dates from {start_date_str} to {end_date_str}")

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "ods", "pg"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    sensor_on_raw_layer = PythonSensor(
        task_id="sensor_on_raw_layer",
        python_callable=was_raw_dag_success_today,
        mode="reschedule",
        poke_interval=60,
        timeout=360000,
    )

    get_and_transfer_raw_data_to_ods_pg = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=get_and_transfer_raw_data_to_ods_pg,      
        # Раскомментировать чтобы передать даты вручную
        op_kwargs={
            "manual_start_date": "2025-11-01",
            "manual_end_date": "2025-11-19",
        },
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> sensor_on_raw_layer >> get_and_transfer_raw_data_to_ods_pg >> end