-- =====================================================
-- Database
-- =====================================================
CREATE DATABASE IF NOT EXISTS wistia_analytics;

-- =====================================================
-- DIM_MEDIA
-- =====================================================
CREATE EXTERNAL TABLE IF NOT EXISTS wistia_analytics.dim_media (
  media_id STRING,
  hashed_id STRING,
  title STRING,
  url STRING,
  created_at STRING
)
STORED AS PARQUET
LOCATION 's3://wistia-data/data_curated/dim_media/';

-- =====================================================
-- DIM_VISITOR
-- =====================================================
CREATE EXTERNAL TABLE IF NOT EXISTS wistia_analytics.dim_visitor (
  visitor_id STRING,
  ip_address STRING,
  country STRING
)
STORED AS PARQUET
LOCATION 's3://wistia-data/data_curated/dim_visitor/';

-- =====================================================
-- FACT_MEDIA_ENGAGEMENT (Partitioned)
-- =====================================================
CREATE EXTERNAL TABLE IF NOT EXISTS wistia_analytics.fact_media_engagement (
  media_id STRING,
  visitor_id STRING,
  play_count INT,
  play_rate DOUBLE,
  total_watch_time DOUBLE,
  watched_percent DOUBLE
)
PARTITIONED BY (
  date DATE
)
STORED AS PARQUET
LOCATION 's3://wistia-data/data_curated/fact_media_engagement/';

-- =====================================================
-- LOAD PARTITIONS
-- =====================================================
MSCK REPAIR TABLE wistia_analytics.fact_media_engagement;

-- =====================================================
-- ANALYTICS QUERIES
-- =====================================================

-- Video Performance Summary
SELECT
  m.title,
  COUNT(DISTINCT f.visitor_id) AS unique_viewers,
  SUM(f.play_count) AS total_plays,
  ROUND(AVG(f.play_rate), 2) AS avg_play_rate,
  ROUND(AVG(f.watched_percent), 2) AS avg_watched_percent
FROM wistia_analytics.fact_media_engagement f
JOIN wistia_analytics.dim_media m
  ON f.media_id = m.media_id
GROUP BY m.title
ORDER BY total_plays DESC;

-- Engagement by Country
SELECT
  v.country,
  COUNT(DISTINCT f.visitor_id) AS viewers,
  ROUND(AVG(f.watched_percent), 2) AS avg_watch_pct
FROM wistia_analytics.fact_media_engagement f
JOIN wistia_analytics.dim_visitor v
  ON f.visitor_id = v.visitor_id
GROUP BY v.country
ORDER BY viewers DESC;

-- Daily Engagement Trend
SELECT
  date,
  SUM(play_count) AS total_plays,
  ROUND(AVG(watched_percent), 2) AS avg_watch_pct
FROM wistia_analytics.fact_media_engagement
GROUP BY date
ORDER BY date;
