# README:
# SPARK_APPLICATION_ARGS contains stock-market/AAPL/prices.json
# SPARK_APPLICATION_ARGS will be passed to the Spark application as an argument -e when running the Spark application from Airflow
# - Sometimes the script can stay stuck after "Passing arguments..."
# - Sometimes the script can stay stuck after "Successfully stopped SparkContext"
# - Sometimes the script can show "WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources"
# The easiest way to solve that is to restart your Airflow instance
# astro dev kill && astro dev start
# Also, make sure you allocated at least 8gb of RAM to Docker Desktop
# Go to Docker Desktop -> Preferences -> Resources -> Advanced -> Memory

# Import the SparkSession module
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import explode, map_entries, to_timestamp,from_json,col
from pyspark.sql.types import DateType, MapType, StringType, StructType, StructField

import os
import sys

if __name__ == '__main__':

    def app():
        # Create a SparkSession
        spark = SparkSession.builder.appName("FormatStock") \
            .config("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minio")) \
            .config("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")) \
            .config("fs.s3a.endpoint", os.getenv("ENDPOINT", "http://host.docker.internal:9000")) \
            .config("fs.s3a.connection.ssl.enabled", "false") \
            .config("fs.s3a.path.style.access", "true") \
            .config("fs.s3a.attempts.maximum", "1") \
            .config("fs.s3a.connection.establish.timeout", "5000") \
            .config("fs.s3a.connection.timeout", "10000") \
            .getOrCreate()

        # 1) Define the schema for the "metrics" struct and the top-level map
        metrics_schema = StructType([
            StructField("1. open",  StringType()),
            StructField("2. high",  StringType()),
            StructField("3. low",   StringType()),
            StructField("4. close", StringType()),
            StructField("5. volume",StringType()),
        ])
        top_map_schema = MapType(StringType(), metrics_schema)
        
        # 2) Read the whole file as text (handles one big JSON object) and parse as a map
        df = spark.read.text(f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}/prices.json")
        parsed = df.select(from_json(col("value"), top_map_schema).alias("tsmap"))

        ts = parsed.select(
            explode(map_entries(col("tsmap"))).alias("entry")
        )

        df_clean = (
            ts.select(
                to_timestamp(col("entry.key")).alias("timestamp"),
                col("entry.value.`1. open`").cast("double").alias("open"),
                col("entry.value.`2. high`").cast("double").alias("high"),
                col("entry.value.`3. low`").cast("double").alias("low"),
                col("entry.value.`4. close`").cast("double").alias("close"),
                col("entry.value.`5. volume`").cast("long").alias("volume"),
            )
            .orderBy("timestamp")
        )

        # Store in Minio
        df_clean.write \
            .mode("overwrite") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .csv(f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}/formatted_prices")
        
    app()
    os.system('kill %d' % os.getpid())