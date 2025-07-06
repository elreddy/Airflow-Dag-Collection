import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

# Get today's date in the format YYYY-MM-DD

today_date = datetime.now().strftime('%Y-%m-%d')

# ──── 1. DAG & default_args ──────────────────────────────────────────────

default_args = {
    'owner': 'Lokesh',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='rentlok_extract_to_staging',
    description='Extract OLTP tables to Lakehouse staging as Parquet',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2025, 6, 8),
    catchup=False,
    max_active_runs=1,
    tags=['rentlok','lakehouse'],
) as dag:
    
 # ──── 3. Dynamic tasks per table ─────────────────────────────────────

    tables = Variable.get("rentlok_source_tables", deserialize_json=True)
    for tbl in tables:

        task2 = SparkSubmitOperator(
            task_id=f"spark_extract_{tbl}",
            application="/home/ereddy/airflow/scripts/extract_to_parquet.py",
            conn_id="spark_conn_rentlok",
            application_args=[
                "--table", tbl,
                "--jdbc-conn", "postgres_conn_rentlok",
                "--output-dir", f"hdfs://master:9000/user/ereddy/datalake/rentlok/staging/{tbl}/{today_date}/"
            ],
            do_xcom_push=True  # so we can grab row_count
        )

        task2