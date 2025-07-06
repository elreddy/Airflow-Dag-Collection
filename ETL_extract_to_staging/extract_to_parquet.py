#!/usr/bin/env python3
import argparse
import logging
from datetime import datetime

from airflow.hooks.base import BaseHook
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

def get_postgres_props(conn_id: str):
    """
    Fetch Postgres connection info from Airflow and build JDBC URL + props.
    """
    pg = BaseHook.get_connection(conn_id)
    jdbc_url = f"jdbc:postgresql://{pg.host}:{pg.port}/{pg.schema}"
    props = {
        "user": pg.login,
        "password": pg.password,
        "driver": "org.postgresql.Driver"
    }
    return jdbc_url, props

def init_spark(app_name: str):
    """
    Initialize a SparkSession with Delta Lake support.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )

def extract_table(spark: SparkSession, table: str, jdbc_url: str, props: dict):
    """
    Read a full table from Postgres via JDBC.
    """
    logging.info(f"Starting extract of {table}")
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", props["user"])
        .option("password", props["password"])
        .option("driver", props["driver"])
        .load()
    )
    count = df.count()
    logging.info(f"Extracted {count} rows from {table}")
    return df, count

def write_parquet(df, output_dir: str):
    """
    Write DataFrame to Parquet, partitioned by run date.
    """
    run_date = datetime.now().strftime("%Y-%m-%d")
    target = output_dir.rstrip("/") + f"/run_date={run_date}/"
    df_with_ts = df.withColumn("extract_ts", current_timestamp())
    logging.info(f"Writing to {target}")
    (
        df_with_ts
        .write
        .mode("overwrite")
        .parquet(target)
    )
    return target

def main():
    parser = argparse.ArgumentParser(description="Extract a Postgres table to Parquet")
    parser.add_argument("--table",      required=True, help="Source Postgres table")
    parser.add_argument("--jdbc-conn",  required=True, help="Airflow Postgres connection ID")
    parser.add_argument("--output-dir", required=True, help="HDFS staging output directory")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    # 1. Get JDBC props via Airflow Hook

    jdbc_url, pg_props = get_postgres_props(args.jdbc_conn)

    # 2. Init Spark

    spark = init_spark(app_name=f"extract_{args.table}")

    # 3. Extract

    df, row_count = extract_table(spark, args.table, jdbc_url, pg_props)

    # 4. Write Parquet

    output_path = write_parquet(df, args.output_dir)

    # 5. Print row_count for XCom parsing

    print(f'{{"row_count": {row_count}}}')
    logging.info(f"Extraction of {args.table} complete; written to {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
