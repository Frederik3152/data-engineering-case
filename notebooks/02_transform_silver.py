# Databricks notebook source

# MAGIC %md
# MAGIC # Silver Transform — ClinicalTrials.gov
# MAGIC
# MAGIC Reads raw JSON from the bronze table, extracts the relevant fields
# MAGIC into typed columns, and deduplicates to one row per `nct_id` (latest ingest only).

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run context (job lineage)

# COMMAND ----------

dbutils.widgets.text("job_id", "manual")
dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("run_time", "")

job_id = dbutils.widgets.get("job_id")
run_id = dbutils.widgets.get("run_id")
run_time_str = dbutils.widgets.get("run_time")
run_time = run_time_str if run_time_str else datetime.now(timezone.utc).isoformat()

print(f"job_id={job_id}  run_id={run_id}  run_time={run_time}")

# COMMAND ----------

CATALOG = "data_engineering_case"
BRONZE_TABLE = f"{CATALOG}.bronze.clinical_trials"
SILVER_TABLE = f"{CATALOG}.silver.clinical_trials"

# Schema for the subset of raw_json moved to silver.
# Fields not listed here are ignored; missing fields become null.
STUDY_SCHEMA = """
    protocolSection STRUCT<
        identificationModule: STRUCT<briefTitle: STRING, officialTitle: STRING>,
        statusModule: STRUCT<
            overallStatus: STRING,
            startDateStruct: STRUCT<date: STRING>,
            completionDateStruct: STRUCT<date: STRING>,
            lastUpdatePostDateStruct: STRUCT<date: STRING>
        >,
        designModule: STRUCT<
            studyType: STRING,
            phases: ARRAY<STRING>,
            enrollmentInfo: STRUCT<count: INT>
        >,
        conditionsModule: STRUCT<conditions: ARRAY<STRING>>,
        descriptionModule: STRUCT<briefSummary: STRING>,
        sponsorCollaboratorsModule: STRUCT<leadSponsor: STRUCT<name: STRING>>
    >
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse and flatten JSON, dedupe on `nct_id`

# COMMAND ----------

# Extract fields from the raw JSON column using the defined schema, focusing on the protocolSection
main_section = F.from_json("raw_json", STUDY_SCHEMA)["protocolSection"]

# Flatten and rename the nested structure
flattened = spark.table(BRONZE_TABLE).select(
    "nct_id",
    main_section["identificationModule"]["briefTitle"].alias("brief_title"),
    main_section["identificationModule"]["officialTitle"].alias("official_title"),
    main_section["statusModule"]["overallStatus"].alias("overall_status"),
    main_section["designModule"]["studyType"].alias("study_type"),
    main_section["designModule"]["phases"].alias("phases"),
    main_section["conditionsModule"]["conditions"].alias("conditions"),
    main_section["descriptionModule"]["briefSummary"].alias("brief_summary"),
    F.to_date(main_section["statusModule"]["startDateStruct"]["date"]).alias("start_date"),
    F.to_date(main_section["statusModule"]["completionDateStruct"]["date"]).alias("completion_date"),
    F.to_date(main_section["statusModule"]["lastUpdatePostDateStruct"]["date"]).alias("last_update_post_date"),
    main_section["designModule"]["enrollmentInfo"]["count"].alias("enrollment_count"),
    main_section["sponsorCollaboratorsModule"]["leadSponsor"]["name"].alias("lead_sponsor_name"),
    "ingested_at",
)

# Deduplicate to one row per `nct_id` by keeping the latest ingest (based on `ingested_at`)
latest_per_study = Window.partitionBy("nct_id").orderBy(F.col("ingested_at").desc())
silver_df = (
    flattened.withColumn("_rn", F.row_number().over(latest_per_study))
    .where("_rn = 1")
    .drop("_rn")
    .withColumn("job_id", F.lit(job_id))
    .withColumn("run_id", F.lit(run_id))
    .withColumn("run_time", F.lit(run_time))
)

# COMMAND ----------

# Overwrite the silver table with the transformed and deduplicated data
silver_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
