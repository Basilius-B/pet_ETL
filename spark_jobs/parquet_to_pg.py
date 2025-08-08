import sys
import logging
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

history_date = sys.argv[1]
symbol = sys.argv[2]
table = sys.argv[3]
logging.info('start spark parquet to pg job')


def parquet_to_pg(history_date, symbol, table):
    logging.info('create spark session')
    spark = SparkSession.builder \
        .appName("parquet_to_pg") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    target_bucket = 'transformed-data'
    object_name = f"5m/{symbol}/{history_date}"
    # Read Parquet from S3
    df = spark.read.parquet(f"s3a://{target_bucket}/{object_name}/")
    target_date = df.selectExpr("date(open_time) as open_date").first()["open_date"]

    # Write to PostgreSQL
    jdbc_url = "jdbc:postgresql://postgres:5432/crypto_data"
    connection_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    existing_keys_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"(SELECT DISTINCT date(open_time) AS open_date FROM {table}) AS existing",
        properties=connection_properties
    )
    existing_dates = [row["open_date"] for row in existing_keys_df.collect()]

    if target_date not in existing_dates:
        logging.info('write to db')
        df.write \
            .jdbc(url=jdbc_url,
                  table=table,
                  mode="append",
                  properties=connection_properties)
        logging.info('success')
    spark.stop()


parquet_to_pg(history_date, symbol, table)
