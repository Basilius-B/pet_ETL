import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

history_date = sys.argv[1]
symbol = sys.argv[2]


def transform_history(history_date, symbol):
    logging.info('create spark session')
    spark = SparkSession.builder \
        .appName("WriteToS3") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    source_bucket = "raw-data"
    target_bucket = 'transformed-data'
    object_name = f"5m/{symbol}/{history_date}"

    logging.info("read parquet to df : %s", object_name)
    df = spark.read.parquet(f"s3a://{source_bucket}/{object_name}.parquet")
    logging.info("transform df : %s", object_name)
    df = (df.withColumn("open_time", to_timestamp((col("open_time") / 1000).cast("timestamp")))
          .withColumn("close_time", to_timestamp((col("close_time") / 1000).cast("timestamp")))
          )
    logging.info("write to s3 : %s/%s", target_bucket, object_name)
    df.write.mode("overwrite").parquet(f"s3a://{target_bucket}/{object_name}/")

    spark.stop()


transform_history(history_date, symbol)
