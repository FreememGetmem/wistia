from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, explode
from pyspark.sql.types import *

spark = SparkSession.builder.appName("wistia-transform").getOrCreate()

RAW = "s3://YOUR-BUCKET/raw/wistia"
DWH = "s3://YOUR-BUCKET/dwh/wistia"

# ----------------------------------------------------------
# Load Raw Data
# ----------------------------------------------------------
media_df = spark.read.json(f"{RAW}/media/*/*.json")
events_df = spark.read.json(f"{RAW}/events/*/*/*.json")
visitors_df = spark.read.json(f"{RAW}/visitors/*/*/*.json")

# ----------------------------------------------------------
# DIM MEDIA
# ----------------------------------------------------------
dim_media = media_df.select(
    col("hashed_id").alias("media_id"),
    col("name").alias("title"),
    col("url"),
    col("created_at")
).dropDuplicates(["media_id"])

dim_media.write.mode("overwrite").parquet(f"{DWH}/dim_media")

# ----------------------------------------------------------
# DIM VISITOR
# ----------------------------------------------------------
dim_visitor = visitors_df.select(
    col("id").alias("visitor_id"),
    col("ip_address"),
    col("country"),
    col("created_at")
).dropDuplicates(["visitor_id"])

dim_visitor.write.mode("overwrite").parquet(f"{DWH}/dim_visitor")

# ----------------------------------------------------------
# FACT MEDIA ENGAGEMENT
# ----------------------------------------------------------
fact = events_df.select(
    col("media_id"),
    col("visitor_id"),
    to_date(col("created_at")).alias("date"),
    col("percent_viewed").alias("watched_percent"),
    col("durations").alias("watch_time"),
    col("action")
)

fact.write.mode("append").partitionBy("date").parquet(f"{DWH}/fact_media_engagement")

spark.stop()
