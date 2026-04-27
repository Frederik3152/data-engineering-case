# Databricks notebook source

# MAGIC %md
# MAGIC # Gold Transform — Trials by Sponsor
# MAGIC
# MAGIC Aggregates the silver table into one row per `lead_sponsor_name`,
# MAGIC with trial counts by status and enrollment summaries.

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
GOLD_TABLE = f"{CATALOG}.gold.trials_by_sponsor"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate trials per sponsor

# COMMAND ----------

silver_df = spark.table(SILVER_TABLE).where(F.col("lead_sponsor_name").isNotNull())

gold_df = (
    silver_df.groupBy("lead_sponsor_name")
    .agg(
        F.count("*").alias("total_trials"),
        F.sum(F.when(F.col("overall_status") == "RECRUITING", 1).otherwise(0)).alias("recruiting_trials"),
        F.sum(F.when(F.col("overall_status") == "COMPLETED", 1).otherwise(0)).alias("completed_trials"),
        F.sum(F.when(F.col("overall_status") == "TERMINATED", 1).otherwise(0)).alias("terminated_trials"),
        F.sum("enrollment_count").alias("total_enrollment"),
        F.round(F.avg("enrollment_count"), 0).cast("int").alias("avg_enrollment"),
        F.min("start_date").alias("earliest_start"),
        F.max("last_update_post_date").alias("latest_update"),
    )
    .withColumn("job_id", F.lit(job_id))
    .withColumn("run_id", F.lit(run_id))
    .withColumn("run_time", F.lit(run_time))
)

# COMMAND ----------

# Overwrite the gold table with the aggregated sponsor metrics
gold_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
