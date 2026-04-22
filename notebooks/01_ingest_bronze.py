# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze Ingestion — ClinicalTrials.gov
# MAGIC
# MAGIC Pulls raw study data from the ClinicalTrials.gov v2 API and writes it
# MAGIC as-is into a Delta table in the bronze layer of our Unity Catalog.

# COMMAND ----------

import json
from datetime import datetime, timezone

import requests

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "main"
SCHEMA = "default"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_clinical_trials"

API_BASE = "https://clinicaltrials.gov/api/v2/studies"
PAGE_SIZE = 100
MAX_PAGES = 10  # ~1,000 studies per run — adjust as needed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch studies from the API

# COMMAND ----------


def fetch_all_studies(page_size: int = PAGE_SIZE, max_pages: int = MAX_PAGES) -> list[dict]:
    """
    Fetch studies from the ClinicalTrials.gov v2 API with pagination.
    """
    all_studies = []
    params = {"pageSize": min(page_size, 1000)}
    page_token = None
    pages_fetched = 0

    while pages_fetched < max_pages:
        if page_token:
            params["pageToken"] = page_token

        resp = requests.get(API_BASE, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        studies = data.get("studies", [])
        if not studies:
            break

        all_studies.extend(studies)
        pages_fetched += 1

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    print(f"Fetched {len(all_studies)} studies across {pages_fetched} pages")
    return all_studies


# COMMAND ----------

studies = fetch_all_studies()

# COMMAND ----------

ingest_ts = datetime.now(timezone.utc).isoformat()

# Build rows: store each study as raw JSON string + metadata
rows = []
for study in studies:
    nct_id = study.get("protocolSection", {}).get("identificationModule", {}).get("nctId", "UNKNOWN")
    rows.append(
        {
            "nct_id": nct_id,
            "raw_json": json.dumps(study),
            "ingested_at": ingest_ts,
        }
    )

print(f"Prepared {len(rows)} rows for bronze table")

# COMMAND ----------

from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame(rows)
df.printSchema()

# COMMAND ----------

# Write (or append) to the bronze Delta table
df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)

print(f"Wrote {df.count()} records to {BRONZE_TABLE}")
