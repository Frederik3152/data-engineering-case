# MAGIC %md
# MAGIC # Utils
# MAGIC Common utility functions for the notebooks.

from datetime import datetime, timezone

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession


def fetch_audit_info():
    """
    Fetch job audit information from Databricks widgets.

    Returns:
    -------------
    Tuple of job_id, run_id, run_time.
    """
    # Pass the SparkSession to DBUtils to access widgets
    dbutils = DBUtils(SparkSession.builder.getOrCreate())

    dbutils.widgets.text("job_id", "manual")
    dbutils.widgets.text("run_id", "manual")
    dbutils.widgets.text("run_time", "")

    job_id = dbutils.widgets.get("job_id")
    run_id = dbutils.widgets.get("run_id")
    run_time_str = dbutils.widgets.get("run_time")
    run_time = run_time_str if run_time_str else datetime.now(timezone.utc).isoformat()

    return job_id, run_id, run_time
