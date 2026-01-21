import json
import os
import time
import logging
from datetime import datetime, timezone
import requests
import boto3
from botocore.exceptions import ClientError, BotoCoreError

# --------------------------------------------------
# Configuration
# --------------------------------------------------
S3_BUCKET = os.environ["S3_BUCKET"]
RAW_PREFIX = os.environ["RAW_PREFIX"]
WATERMARK_TABLE = os.environ["WATERMARK_TABLE"]
WISTIA_SECRET_NAME = os.environ["WISTIA_SECRET_NAME"]
MEDIA_IDS = os.environ["MEDIA_IDS"].split(",")

WISTIA_BASE_URL = "https://api.wistia.com/v1/stats"

# --------------------------------------------------
# Clients
# --------------------------------------------------
s3 = boto3.client("s3")
ddb = boto3.client("dynamodb")
secrets = boto3.client("secretsmanager")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------------------------------------
# Helpers
# --------------------------------------------------



def get_wistia_token():
    try:
        logger.info("Fetching Wistia API token from Secrets Manager")
        response = secrets.get_secret_value(SecretId=WISTIA_SECRET_NAME)
        secret = json.loads(response["SecretString"])
        token = secret["WISTIA_API_TOKEN"]
        logger.info("Successfully retrieved Wistia API token")
        return token
    except (ClientError, KeyError, json.JSONDecodeError) as e:
        logger.error(f"Error fetching Wistia token: {e}")
        raise


def get_watermark(entity):
    try:
        logger.debug(f"Fetching watermark for entity={entity}")
        resp = ddb.get_item(
            TableName=WATERMARK_TABLE,
            Key={"entity": {"S": entity}},
        )
        if "Item" in resp:
            watermark = resp["Item"]["last_updated"]["S"]
            logger.debug(f"Found watermark for {entity}: {watermark}")
            return watermark
        logger.debug(f"No watermark found for {entity}")
    except ClientError as e:
        logger.error(f"DynamoDB get_item error for {entity}: {e}")
    return None


def update_watermark(entity, timestamp):
    try:
        logger.info(f"Updating watermark for entity={entity} to {timestamp}")
        ddb.put_item(
            TableName=WATERMARK_TABLE,
            Item={
                "entity": {"S": entity},
                "last_updated": {"S": timestamp},
            },
        )
        logger.info(f"Watermark updated successfully for {entity}")
    except (ClientError, BotoCoreError) as e:
        logger.error(f"Failed to update watermark for {entity}: {e}")


def fetch_paginated(url, headers, params=None):
    results = []
    page = 1

    while True:
        p = params.copy() if params else {}
        p["page"] = page

        logger.debug(f"Fetching page {page} from {url} with params={p}")
        try:
            r = requests.get(url, headers=headers, params=p, timeout=30)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            break

        data = r.json()
        if not data:
            logger.debug(f"No data returned for page {page}")
            break

        results.extend(data)
        logger.debug(f"Fetched {len(data)} records from page {page}")
        page += 1
        time.sleep(0.3)  # rate limit protection

    logger.info(f"Fetched total {len(results)} records from {url}")
    return results

# --------------------------------------------------
# Lambda Handler
# --------------------------------------------------


def lambda_handler(event, context):
    logger.info("Starting Wistia ingestion job")

    token = get_wistia_token()
    headers = {"Authorization": f"Bearer {token}"}

    run_ts = datetime.now(timezone.utc).isoformat()
    logger.info(f"Job timestamp: {run_ts}")

    for media_id in MEDIA_IDS:
        logger.info(f"Processing media_id={media_id}")

        watermark = get_watermark(media_id)

        # -----------------------------
        # Media-level stats
        # -----------------------------
        media_url = f"{WISTIA_BASE_URL}/medias/{media_id}.json"
        try:
            media_resp = requests.get(media_url, headers=headers)
            media_resp.raise_for_status()
            media_stats = media_resp.json()
            logger.info(f"Retrieved media stats for media_id={media_id}")
        except requests.RequestException as e:
            logger.error(f"Failed to fetch media stats for {media_id}: {e}")
            continue

        # -----------------------------
        # Visitor-level stats
        # -----------------------------
        visitors_url = f"{WISTIA_BASE_URL}/medias/{media_id}/visitors.json"
        params = {}
        if watermark:
            params["updated_after"] = watermark

        visitors = fetch_paginated(visitors_url, headers, params)

        payload = {
            "media_id": media_id,
            "run_timestamp": run_ts,
            "media_stats": media_stats,
            "visitors": visitors,
        }

        s3_key = (
            f"{RAW_PREFIX}/media_id={media_id}/"
            f"ingest_date={run_ts[:10]}/"
            f"{int(time.time())}.json"
        )

        try:
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=json.dumps(payload),
            )
            logger.info(f"Wrote raw data to s3://{S3_BUCKET}/{s3_key}")
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to write S3 object for {media_id}: {e}")
            continue

        update_watermark(media_id, run_ts)

    logger.info("Wistia ingestion completed successfully")
    return {"status": "SUCCESS"}
