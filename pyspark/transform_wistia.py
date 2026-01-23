import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# ----------------------------------------------------------
# Logging Configuration
# ----------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ----------------------------------------------------------
# Spark Session
# ----------------------------------------------------------
logger.info("Starting Spark session")
spark = SparkSession.builder.appName("wistia-transform").getOrCreate()

RAW = "s3://wistia-data/data_raw/"
DWH = "s3://wistia-data/data_curated/"

# ----------------------------------------------------------
# Load Raw Data
# ----------------------------------------------------------
logger.info("Loading raw data")
try:
    media_df = spark.read.json(f"{RAW}/media/*/*.json")
    logger.info(f"Loaded media_df with {media_df.count()} records")
except Exception as e:
    logger.exception(f"Failed to load media_df: {e}")
    raise

try:
    events_df = spark.read.json(f"{RAW}/events/*/*/*.json")
    logger.info(f"Loaded events_df with {events_df.count()} records")
except Exception as e:
    logger.exception(f"Failed to load events_df: {e}")
    raise

try:
    visitors_df = spark.read.json(f"{RAW}/visitors/*/*/*.json")
    logger.info(f"Loaded visitors_df with {visitors_df.count()} records")
except Exception as e:
    logger.exception(f"Failed to load visitors_df: {e}")
    raise

# ----------------------------------------------------------
# DIM MEDIA
# ----------------------------------------------------------
logger.info("Transforming DIM MEDIA")
try:
    dim_media = media_df.select(
        col("hashed_id").alias("media_id"),
        col("name").alias("title"),
        col("url"),
        col("created_at")
    ).dropDuplicates(["media_id"])
    logger.info(f"dim_media has {dim_media.count()} unique records")

    dim_media.write.mode("overwrite").parquet(f"{DWH}/dim_media")
    logger.info(f"dim_media written to {DWH}/dim_media")
except Exception as e:
    logger.exception(f"Failed to transform or write dim_media: {e}")
    raise

# ----------------------------------------------------------
# DIM VISITOR
# ----------------------------------------------------------
logger.info("Transforming DIM VISITOR")
try:
    dim_visitor = visitors_df.select(
        col("id").alias("visitor_id"),
        col("ip_address"),
        col("country"),
        col("created_at")
    ).dropDuplicates(["visitor_id"])
    logger.info(f"dim_visitor has {dim_visitor.count()} unique records")

    dim_visitor.write.mode("overwrite").parquet(f"{DWH}/dim_visitor")
    logger.info(f"dim_visitor written to {DWH}/dim_visitor")
except Exception as e:
    logger.exception(f"Failed to transform or write dim_visitor: {e}")
    raise

# ----------------------------------------------------------
# FACT MEDIA ENGAGEMENT
# ----------------------------------------------------------
logger.info("Transforming FACT MEDIA ENGAGEMENT")
try:
    fact = events_df.select(
        col("media_id"),
        col("visitor_id"),
        to_date(col("created_at")).alias("date"),
        col("percent_viewed").alias("watched_percent"),
        col("durations").alias("watch_time"),
        col("action")
    )
    logger.info(f"fact_media_engagement has {fact.count()} records")

    fact.write.mode("append").partitionBy("date").parquet(f"{DWH}/fact_media_engagement")
    logger.info(f"fact_media_engagement written to {DWH}/fact_media_engagement with partitioning by date")
except Exception as e:
    logger.exception(f"Failed to transform or write fact_media_engagement: {e}")
    raise

# ----------------------------------------------------------
# Stop Spark Session
# ----------------------------------------------------------
logger.info("Stopping Spark session")
spark.stop()
logger.info("Spark job completed successfully ")
