# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Ingestion: Raw CSV to Delta
# MAGIC **Purpose:** 
# MAGIC This notebook represents the Bronze layer of our Medallion architecture. It extracts a raw CSV file from Azure Data Lake Storage (ADLS), converts it into a Delta format, and stores it as an External Table.
# MAGIC
# MAGIC **Key Features:**
# MAGIC * Centralized configuration for easy environment switching (Dev/Prod).
# MAGIC * Parameterized for Orchestration (Azure Data Factory).
# MAGIC * Custom Audit Logging for pipeline observability.
# MAGIC * Idempotent design using `CREATE TABLE IF NOT EXISTS` and `Overwrite` mode.

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

# ---------------------------------------------------------
# CONFIGURATION
# Centralizing paths avoids hardcoding inside the logic
# ---------------------------------------------------------
CONFIG = {
    "source_path": "abfss://project@ecomstora.dfs.core.windows.net/rawdata/Ecommerce_2000.csv",
    "target_table": "ecomwork.bronze.orders_file_raw",
    "target_path": "abfss://project@ecomstora.dfs.core.windows.net/processed/bronze/orders_file_raw",
    "log_table": "ecomwork.audit.pipeline_log",
    "step_name": "bronze_orders_file"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Parameters & Setup
# MAGIC We use `dbutils.widgets` to allow Azure Data Factory (ADF) to pass dynamic parameters into this notebook at runtime. 
# MAGIC * If ran via ADF, it uses the orchestrator's `run_id`.
# MAGIC * If ran manually for testing, it auto-generates a `run_id` based on the current UTC timestamp.

# COMMAND ----------

# Define widgets to accept parameters from ADF
dbutils.widgets.text("pipeline_name", "ecom_pipeline")
dbutils.widgets.text("run_id", "")

PIPELINE_NAME = dbutils.widgets.get("pipeline_name")
_run_id_widget = dbutils.widgets.get("run_id")

def get_utc_now():
    """Returns timezone-naive UTC datetime for Spark TIMESTAMP compatibility."""
    return datetime.now(timezone.utc).replace(tzinfo=None)

# Resolve Run ID
RUN_ID = _run_id_widget if _run_id_widget else str(int(get_utc_now().timestamp()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Audit Logging Framework
# MAGIC Instead of just printing success/failure to the console, we write audit events to a centralized Delta table (`ecomwork.audit.pipeline_log`). 
# MAGIC
# MAGIC This tracks:
# MAGIC * Execution time (Start/End)
# MAGIC * Row counts
# MAGIC * Errors/Stacktraces (if the pipeline fails)

# COMMAND ----------

# Define strictly typed schema for the logging table
LOG_SCHEMA = StructType([
    StructField("pipeline_name", StringType(), True),
    StructField("run_id",        StringType(), True),
    StructField("step_name",     StringType(), True),
    StructField("status",        StringType(), True),
    StructField("start_ts",      TimestampType(), True),
    StructField("end_ts",        TimestampType(), True),
    StructField("source",        StringType(), True),
    StructField("target",        StringType(), True),
    StructField("row_count",     LongType(), True),
    StructField("error_message", StringType(), True),
])

def log_audit_event(status, start_ts, end_ts=None, row_count=None, error=None):
    """Appends a pipeline execution event to the central audit table."""
    data = [(
        PIPELINE_NAME, 
        RUN_ID, 
        CONFIG["step_name"], 
        status,
        start_ts, 
        end_ts, 
        CONFIG["source_path"], 
        CONFIG["target_table"],
        None if row_count is None else int(row_count),
        error
    )]
    
    (spark.createDataFrame(data, schema=LOG_SCHEMA)
     .write.mode("append")
     .saveAsTable(CONFIG["log_table"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Core ETL Logic
# MAGIC This is the main ingestion function. 
# MAGIC
# MAGIC **Best Practices implemented here:**
# MAGIC 1. **Try/Except Block**: Catches failures, logs them to the audit table, then re-raises them so ADF knows the job failed.
# MAGIC 2. **External Tables**: We use `LOCATION` to define an external table. This prevents Unity Catalog managed-storage access issues and keeps storage lifecycle separate from compute.
# MAGIC 3. **Caching (`df.cache()`)**: Because `count()` is an action, caching prevents Spark from reading the CSV twice (once for count, once for write).
# MAGIC 4. **Idempotency**: Using `mode("overwrite")` ensures rerunning the pipeline doesn't duplicate data.

# COMMAND ----------

def load_bronze_layer():
    """Reads raw data, writes to external Delta table, and manages logging."""
    start_time = get_utc_now()
    log_audit_event("STARTED", start_ts=start_time)

    try:
        # 1. Read Data (Infer schema for Bronze layer flexibility)
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(CONFIG["source_path"]))

        # 2. Optimize & Count
        df.cache() 
        row_count = df.count()

        # 3. Create External Table (if it doesn't exist)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {CONFIG["target_table"]}
            USING DELTA
            LOCATION '{CONFIG["target_path"]}'
        """)

        # 4. Write Data (Idempotent Overwrite)
        (df.write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .save(CONFIG["target_path"]))
         
        # Clear cache to free memory
        df.unpersist()

        # 5. Log Success
        log_audit_event("SUCCESS", start_ts=start_time, end_ts=get_utc_now(), row_count=row_count)
        print(f" Successfully loaded {row_count} rows into {CONFIG['target_table']}.")

    except Exception as e:
        # 6. Log Failure & bubble up to Orchestrator
        error_msg = str(e)[:1000] # Truncate to fit schema limits
        log_audit_event("FAILED", start_ts=start_time, end_ts=get_utc_now(), error=error_msg)
        print(f" Pipeline Failed: {error_msg}")
        raise e # Re-raise for ADF to catch

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execution Trigger
# MAGIC Standard Python entry point to trigger the ETL process.

# COMMAND ----------

if __name__ == "__main__":
    load_bronze_layer()