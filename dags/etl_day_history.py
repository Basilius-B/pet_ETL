import io
import logging
import os
import zipfile

import psycopg2
import requests
import pandas as pd
import pyarrow as pa
from airflow.operators.empty import EmptyOperator
from pandas.core.ops.dispatch import should_extension_dispatch
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from botocore.exceptions import ClientError
from pyarrow import parquet
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import boto3
from datetime import datetime

url = "https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/5m/"

client = boto3.client(
    service_name='s3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minio',
    aws_secret_access_key='minio123',
)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=60),
}

["BTCUSDT", "DOGEUSDT", "ETHUSDT"]

list_pair = Variable.get("list_symbol", deserialize_json=True)

dag = DAG(
    "etl_5m_history",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    catchup=False,
    description='ETL DAG 5m kline price',
)


def date_range(start_date, end_date):
    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    while current <= end:
        yield current.strftime('%Y-%m-%d')
        current += timedelta(days=1)


def load_raw_data(history_date, symbol, **kwargs):
    logging.info(f"start {symbol}")
    zip_url = f'https://data.binance.vision/data/futures/um/daily/klines/{symbol}/5m/{symbol}-5m-{history_date}.zip'  # Replace with your real URL
    response = requests.get(zip_url)
    if response.status_code != 200:
        logging.error(f"Failed to download ZIP: {response.status_code}")
        raise Exception(f"Failed to download ZIP: {response.status_code}")
    zip_bytes = io.BytesIO(response.content)
    bucket_name = "raw-data"
    object_name = f"5m/{symbol}/{history_date}.parquet"
    with zipfile.ZipFile(zip_bytes) as zf:
        csv_name = next((name for name in zf.namelist() if name.endswith('.csv')), None)
        if not csv_name:
            raise Exception("No CSV file found in ZIP.")
        with zf.open(csv_name) as csv_file:
            df = pd.read_csv(csv_file)

    buffer = io.BytesIO()
    table = pa.Table.from_pandas(df)
    parquet.write_table(table, buffer)
    buffer.seek(0)
    try:
        client.head_object(Bucket=bucket_name, Key=object_name)
        print(f"❌ File already exists in s3://{bucket_name}/{object_name}")
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            # Объект не найден — можно загружать
            client.upload_fileobj(buffer, bucket_name, object_name)
            print(f"✅ Uploaded to s3://{bucket_name}/{object_name}")
        else:
            # Другие ошибки (например, прав нет)
            raise


def create_table_if_needed(symbol):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {symbol}_5m_kline(
    open_time TIMESTAMP PRIMARY KEY,
    open NUMERIC(18, 4),
    high NUMERIC(18, 4),
    low NUMERIC(18, 4),
    close NUMERIC(18, 4),
    volume NUMERIC(20, 6),
    close_time TIMESTAMP,
    quote_volume NUMERIC(20, 6),
    count INTEGER,
    taker_buy_volume NUMERIC(20, 6),
    taker_buy_quote_volume NUMERIC(20, 6),
    ignore INTEGER
);
    """
    conn = psycopg2.connect(
        dbname="crypto_data",
        user="airflow",
        password="airflow",
        host="postgres",
        port=5432
    )
    cur = conn.cursor()
    cur.execute(ddl)
    conn.commit()
    cur.close()
    conn.close()


start = EmptyOperator(task_id="start")
stop = EmptyOperator(task_id="stop")

list_task = [start >>
             PythonOperator(
                 task_id=f'extract_{symbol}',
                 python_callable=load_raw_data,
                 provide_context=True,
                 dag=dag,
                 op_kwargs={
                     'history_date': (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d'),
                     'symbol': symbol
                 }
             ) >>
             SparkSubmitOperator(
                 task_id=f'transform_{symbol}',
                 conn_id="spark_default",
                 application="/opt/airflow/spark_jobs/5m_transform.py",
                 conf={"spark.executor.memory": "1g",
                       "spark.submit.deployMode": "client"},
                 application_args=[
                     (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d'),
                     symbol
                 ],
                 jars="/opt/bitnami/spark/custom-jars/hadoop-aws-3.3.4.jar,"
                      "/opt/bitnami/spark/custom-jars/aws-java-sdk-bundle-1.11.1026.jar",
                 name="transform-history",
                 verbose=True,
             ) >>
             PythonOperator(
                 task_id=f"create_pg_table_{symbol}",
                 python_callable=create_table_if_needed,
                 dag=dag,
                 op_kwargs={
                     'history_date': (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d'),
                     'symbol': symbol
                 }
             ) >>
             SparkSubmitOperator(
                 task_id=f'load_to_pg_{symbol}',
                 conn_id="spark_default",
                 application="/opt/airflow/spark_jobs/parquet_to_pg.py",
                 conf={"spark.executor.memory": "1g",
                       "spark.submit.deployMode": "client"},
                 application_args=[
                     (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d'),
                     symbol,
                     f"{symbol}_5m_kline"
                 ],
                 jars="/opt/bitnami/spark/custom-jars/hadoop-aws-3.3.4.jar,"
                      "/opt/bitnami/spark/custom-jars/aws-java-sdk-bundle-1.11.1026.jar,"
                      "/opt/bitnami/spark/custom-jars/postgresql-42.7.3.jar",
                 name="parquet_to_pg",
                 verbose=True,
             )
             for symbol in list_pair]

list_task >> stop
