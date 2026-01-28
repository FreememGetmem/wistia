import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, explode
from pyspark.sql.functions import current_date

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

RAW = "s3://wistia-pipeline-635374934580-us-west-1/data_raw/"
DWH = "s3://wistia-pipeline-635374934580-us-west-1/data_curated/"

# ----------------------------------------------------------
# Load Raw Data
# ----------------------------------------------------------
logger.info("Loading raw data")
try:
    media_df = spark.read.json(f"{RAW}/media_id=*/ingest_date=*/*.json")
    logger.info(f"Loaded media_df with {media_df.count()} records")
except Exception as e:
    logger.exception(f"Failed to load media_df: {e}")
    raise

""" try:
    events_df = spark.read.json(f"{RAW}/event_type=events/media_id=*/ingest_date=*/*.json")
    logger.info(f"Loaded events_df with {events_df.count()} records")
except Exception as e:
    logger.exception(f"Failed to load events_df: {e}")
    raise

try:
    visitors_df = (spark.read.option("basePath", RAW).json(f"{RAW}/event_type=visitors/media_id=*/ingest_date=*/*.json"))
    logger.info(f"Loaded visitors_df with {visitors_df.count()} records")
except Exception as e:
    logger.exception(f"Failed to load visitors_df: {e}")
    raise """

# ----------------------------------------------------------
# DIM MEDIA
# ----------------------------------------------------------
logger.info("Transforming DIM MEDIA")
try:
    dim_media = media_df.select(
                                col("media_id"),
                                col("media_stats.engagement").alias("engagement"),
                                col("media_stats.hours_watched"),
                                col("media_stats.load_count"),
                                col("media_stats.play_count"),
                                col("media_stats.play_rate")
                            ).dropDuplicates(["media_id"])
    logger.info(f"dim_media has {dim_media.count()} unique records")

    dim_media.write.mode("overwrite").parquet(f"{DWH}/dim_media")
    logger.info(f"dim_media written to {DWH}/dim_media")
    logger.info(media_df.printSchema())
except Exception as e:
    logger.exception(f"Failed to transform or write dim_media: {e}")
    raise

# ----------------------------------------------------------
# DIM VISITOR
# ----------------------------------------------------------
logger.info("Transforming DIM VISITOR")
try:
    dim_visitor = (
                    media_df.select(
                        col("media_id"),
                        explode(col("visitors")).alias("visitor")  # explode the visitors array
                    )
                    .select(
                        col("media_id"),
                        col("visitor.visitor_key").alias("visitor_id"),  # use visitor_key instead of id
                        col("visitor.ip").alias("ip_address"),
                        col("visitor.country"),
                        col("visitor.received_at").alias("created_at")
                    )
                ).dropDuplicates(["visitor_id"])
    logger.info(f"dim_visitor has {dim_visitor.count()} unique records")
    logger.info(dim_visitor.printSchema())
    dim_visitor.write.mode("overwrite").parquet(f"{DWH}/dim_visitor")
    logger.info(f"dim_visitor written to {DWH}/dim_visitor")
except Exception as e:
    logger.exception(f"Failed to transform or write dim_visitor: {e}")
    raise

# ----------------------------------------------------------
# FACT MEDIA ENGAGEMENT
# ----------------------------------------------------------
logger.info("Transforming FACT MEDIA ENGAGEMENT")
from pyspark.sql.functions import to_date

try:
    fact_media_engagement = (
        media_df.select(
            col("media_id"),
            to_date(col("run_timestamp")).alias("date"),  # use run_timestamp as date
            col("media_stats.engagement"),
            col("media_stats.hours_watched"),
            col("media_stats.load_count"),
            col("media_stats.play_count"),
            col("media_stats.play_rate")
        )
    )

    logger.info(f"fact_media_engagement has {fact_media_engagement.count()} records")
    logger.info(fact_media_engagement.printSchema())
    fact_media_engagement.write.mode("append") \
        .partitionBy("date") \
        .parquet(f"{DWH}/fact_media_engagement")

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
