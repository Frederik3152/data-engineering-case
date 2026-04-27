# Databricks notebook source

# MAGIC %md
# MAGIC # Gold Transform — Trials by Condition
# MAGIC
# MAGIC Explodes the `conditions` array from silver and aggregates into one row
# MAGIC per condition, with trial counts by status and enrollment summaries.

# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils import fetch_audit_info

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run context (job lineage)

# COMMAND ----------

job_id, run_id, run_time = fetch_audit_info()

# COMMAND ----------

CATALOG = "data_engineering_case"
SILVER_TABLE = f"{CATALOG}.silver.clinical_trials"
GOLD_TABLE = f"{CATALOG}.gold.trials_by_condition"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explode conditions and aggregate per condition

# COMMAND ----------

# One row per (nct_id, condition); trials with no conditions are dropped
exploded = spark.table(SILVER_TABLE).select(
    "nct_id",
    "overall_status",
    "enrollment_count",
    "start_date",
    "last_update_post_date",
    F.explode("conditions").alias("condition"),
)

gold_df = (
    exploded.groupBy("condition")
    .agg(
        F.count("*").alias("total_trials"),
        F.sum(F.when(F.col("overall_status") == "RECRUITING", 1).otherwise(0)).alias("recruiting_trials"),
        F.sum(F.when(F.col("overall_status") == "COMPLETED", 1).otherwise(0)).alias("completed_trials"),
        F.sum(F.when(F.col("overall_status") == "TERMINATED", 1).otherwise(0)).alias("terminated_trials"),
        F.round(F.avg("enrollment_count"), 0).cast("int").alias("avg_enrollment"),
        F.min("start_date").alias("earliest_start"),
        F.max("last_update_post_date").alias("latest_update"),
    )
    .withColumn("job_id", F.lit(job_id))
    .withColumn("run_id", F.lit(run_id))
    .withColumn("run_time", F.lit(run_time))
)

# COMMAND ----------

# Overwrite the gold table with the aggregated condition metrics
gold_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
