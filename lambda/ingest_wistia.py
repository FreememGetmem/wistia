import os
import json
import boto3
import requests
from datetime import datetime, timezone
from botocore.exceptions import ClientError

# --- AWS Resources ---
s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")
dynamodb = boto3.resource("dynamodb")

# --- Environment Variables ---
S3_BUCKET = os.environ["S3_BUCKET"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/wistia")
SECRET_NAME = os.environ["WISTIA_SECRET_NAME"]
MEDIA_IDS = os.environ.get("MEDIA_IDS", "gskhw4w4lm,v08dlrgr7v").split(",")

WATERMARK_TABLE = os.environ["WATERMARK_TABLE"]
table = dynamodb.Table(WATERMARK_TABLE)


# ========================================================
# Helper Functions
# ========================================================

def get_api_token():
    """Retrieve Wistia API key from Secrets Manager."""
    secret = secrets.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(secret["SecretString"])["WISTIA_API_TOKEN"]


def get_watermark(entity):
    """Retrieve last updated timestamp for incremental loads."""
    try:
        resp = table.get_item(Key={"entity": entity})
        return resp.get("Item", {}).get("watermark")
    except ClientError:
        return None


def set_watermark(entity, timestamp):
    """Update watermark after each run."""
    table.put_item(Item={"entity": entity, "watermark": timestamp})


def write_raw_to_s3(content, key):
    """Write raw JSON response to S3."""
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(content, default=str),
        ContentType="application/json"
    )


def paginate(url, headers, params=None):
    """Generator that handles pagination using ?page and ?per_page."""
    params = params or {}
    params.setdefault("page", 1)
    params.setdefault("per_page", 100)

    while True:
        res = requests.get(url, headers=headers, params=params, timeout=30)
        res.raise_for_status()
        data = res.json()

        yield data

        # If fewer than per_page items, pagination ends
        if not isinstance(data, list) or len(data) < params["per_page"]:
            break
        params["page"] += 1


# ========================================================
# Main Business Logic
# ========================================================

def fetch_media_data(api_token, run_timestamp):
    """Ingest media-level stats for each media ID."""
    headers = {"Authorization": f"Bearer {api_token}"}

    for mid in MEDIA_IDS:
        url = f"https://api.wistia.com/v1/stats/medias/{mid}.json"
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()

        key = f"{RAW_PREFIX}/media/{mid}/{run_timestamp}.json"
        write_raw_to_s3(data, key)


def fetch_paginated_entity(entity_name, url, api_token, run_date):
    """Ingest events and visitors with pagination & incremental logic."""
    headers = {"Authorization": f"Bearer {api_token}"}
    watermark = get_watermark(entity_name)
    max_ts = watermark

    for page in paginate(url, headers):
        filtered = []

        for item in page:
            ts = item.get("updated_at") or item.get("created_at")
            if watermark and ts and ts <= watermark:
                continue

            filtered.append(item)

            if ts and (max_ts is None or ts > max_ts):
                max_ts = ts

        if filtered:
            key = f"{RAW_PREFIX}/{entity_name}/{run_date}/{datetime.now().isoformat()}.json"
            write_raw_to_s3(filtered, key)

    if max_ts:
        set_watermark(entity_name, max_ts)


# ========================================================
# Lambda Handler
# ========================================================

def lambda_handler(event, context):
    run_timestamp = datetime.now(timezone.utc).isoformat()
    run_date = run_timestamp[:10]

    api_token = get_api_token()

    # Media ingestion
    fetch_media_data(api_token, run_timestamp)

    # Events ingestion
    fetch_paginated_entity(
        "events",
        "https://api.wistia.com/v1/stats/events.json",
        api_token,
        run_date
    )

    # Visitors ingestion
    fetch_paginated_entity(
        "visitors",
        "https://api.wistia.com/v1/stats/visitors.json",
        api_token,
        run_date
    )

    return {
        "status": "SUCCESS",
        "timestamp": run_timestamp,
    }
