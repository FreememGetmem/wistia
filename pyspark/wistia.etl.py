from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    count,
    avg,
    sum as spark_sum,
    when,
    lit
)

# --------------------------------------------------
# Spark Session
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("Wistia-Transform") \
    .getOrCreate()

# --------------------------------------------------
# S3 Paths
# --------------------------------------------------
RAW_BASE = "s3://wistia-data/data_raw/"
DWH_BASE = "s3://wistia-data/data_curated/"

MEDIA_PATH = f"{RAW_BASE}/media/*/*.json"
EVENTS_PATH = f"{RAW_BASE}/events/*/*/*.json"
VISITORS_PATH = f"{RAW_BASE}/visitors/*/*/*.json"

# --------------------------------------------------
# Load Raw Data
# --------------------------------------------------
media_raw = spark.read.json(MEDIA_PATH)
events_raw = spark.read.json(EVENTS_PATH)
visitors_raw = spark.read.json(VISITORS_PATH)

# ==================================================
# DIM_MEDIA
# ==================================================
dim_media = (
    media_raw
    .select(
        col("hashed_id").alias("media_id"),
        col("name").alias("title"),
        col("url"),
        when(col("type").isNotNull(), col("type"))
            .otherwise(lit("unknown"))
            .alias("channel"),
        col("created_at")
    )
    .dropDuplicates(["media_id"])
)

dim_media.write \
    .mode("overwrite") \
    .parquet(f"{DWH_BASE}/dim_media")

# ==================================================
# DIM_VISITOR
# ==================================================
dim_visitor = (
    visitors_raw
    .select(
        col("id").alias("visitor_id"),
        col("ip_address"),
        col("country")
    )
    .dropDuplicates(["visitor_id"])
)

dim_visitor.write \
    .mode("overwrite") \
    .parquet(f"{DWH_BASE}/dim_visitor")

# ==================================================
# FACT_MEDIA_ENGAGEMENT
# ==================================================
fact_media_engagement = (
    events_raw
    .filter(col("media_id").isNotNull())
    .filter(col("visitor_id").isNotNull())
    .withColumn("date", to_date(col("created_at")))
    .groupBy(
        col("media_id"),
        col("visitor_id"),
        col("date")
    )
    .agg(
        # Number of play events
        count(
            when(col("action") == "play", True)
        ).alias("play_count"),

        # Play rate (plays / total events)
        (
            count(when(col("action") == "play", True)) /
            count(lit(1))
        ).alias("play_rate"),

        # Total watch time (seconds)
        spark_sum(
            when(col("watch_time").isNotNull(), col("watch_time"))
            .otherwise(lit(0))
        ).alias("total_watch_time"),

        # Average % watched
        avg(
            when(col("percent_viewed").isNotNull(), col("percent_viewed"))
        ).alias("watched_percent")
    )
)

fact_media_engagement.write \
    .mode("append") \
    .partitionBy("date") \
    .parquet(f"{DWH_BASE}/fact_media_engagement")

# --------------------------------------------------
# End Job
# --------------------------------------------------
spark.stop()
