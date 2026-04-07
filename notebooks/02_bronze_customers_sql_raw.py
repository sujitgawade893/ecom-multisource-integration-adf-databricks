# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM ecomwork.bronze.customers_sql_raw;
# MAGIC SELECT * FROM ecomwork.audit.pipeline_log ORDER BY start_ts DESC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer Ingestion: Azure SQL to Delta
# MAGIC **Purpose:** 
# MAGIC This notebook extracts customer data from an Azure SQL Database via JDBC and loads it into the Bronze layer as an External Delta Table.
# MAGIC
# MAGIC **Key Interview Talking Points:**
# MAGIC * **Security:** Credentials are (or should be) fetched dynamically using `dbutils.secrets.get()`, integrated with Azure Key Vault, rather than hardcoded.
# MAGIC * **Connectivity:** Utilizes Spark's JDBC reader to connect to external RDBMS systems.
# MAGIC * **Performance:** Implements `df.cache()` to prevent multiple network calls to the source database when counting and writing.
# MAGIC * **Idempotency:** Uses `Overwrite` mode and External Tables (`CREATE TABLE IF NOT EXISTS`) to ensure reruns do not duplicate data.

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

# ---------------------------------------------------------
# CONFIGURATION
# Centralizing parameters makes the code modular and clean
# ---------------------------------------------------------
CONFIG = {
    # Source (Azure SQL)
    "jdbc_url": "jdbc:sqlserver://ecomserver.database.windows.net:1433;databaseName=ecomdatabase;encrypt=true;trustServerCertificate=false;loginTimeout=60;",
    "jdbc_driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "source_table": "dbo.customers",
    "source_logical_name": "azuresql:ecomserver/ecomdatabase.dbo.customers", # Used for logging
    
    # Target (Bronze Delta)
    "target_table": "ecomwork.bronze.customers_sql_raw",
    "target_path": "abfss://project@ecomstora.dfs.core.windows.net/processed/bronze/customers_sql_raw",
    
    # Audit
    "log_table": "ecomwork.audit.pipeline_log",
    "step_name": "bronze_customers_sql"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Parameters & Security Setup
# MAGIC Here we accept orchestration parameters from Azure Data Factory (ADF). 
# MAGIC
# MAGIC **Security Best Practice:** 
# MAGIC We prepare the JDBC properties dictionary here. In a production environment, we never hardcode passwords. We use `dbutils.secrets.get()` to securely fetch credentials from an Azure Key Vault backed secret scope.

# COMMAND ----------

# 1. Orchestration Parameters (ADF)
dbutils.widgets.text("pipeline_name", "ecom_pipeline")
dbutils.widgets.text("run_id", "")

PIPELINE_NAME = dbutils.widgets.get("pipeline_name")
_run_id_widget = dbutils.widgets.get("run_id")

def get_utc_now():
    """Returns timezone-naive UTC datetime for Spark TIMESTAMP compatibility."""
    return datetime.now(timezone.utc).replace(tzinfo=None)

RUN_ID = _run_id_widget if _run_id_widget else str(int(get_utc_now().timestamp()))

# 2. Security / JDBC Setup
db_user = "admin123"      
db_password = "xxxxxxx"  
JDBC_PROPERTIES = {
    "user": db_user,
    "password": db_password,
    "driver": CONFIG["jdbc_driver"]
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Audit Logging Framework
# MAGIC We maintain full observability by writing a log record into our central `pipeline_log` table. It captures exactly what was read from Azure SQL and written to ADLS.

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
        CONFIG["source_logical_name"], 
        CONFIG["target_table"],
        None if row_count is None else int(row_count),
        error
    )]
    
    (spark.createDataFrame(data, schema=LOG_SCHEMA)
     .write.mode("append")
     .saveAsTable(CONFIG["log_table"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Core ETL Logic: JDBC to Delta
# MAGIC This is the core execution logic. 
# MAGIC
# MAGIC **Why caching is critical here:** 
# MAGIC Spark evaluates lazily. If we do `df.count()` and then `df.write`, Spark will actually query the source Azure SQL database *twice*. By adding `df.cache()`, we bring the data into Spark memory once, optimizing database load and pipeline speed.

# COMMAND ----------

def load_bronze_sql_layer():
    """Reads from Azure SQL, writes to external Delta table, and manages logging."""
    start_time = get_utc_now()
    log_audit_event("STARTED", start_ts=start_time)

    try:
        # 1. Read Data via JDBC
        df = spark.read.jdbc(
            url=CONFIG["jdbc_url"], 
            table=CONFIG["source_table"], 
            properties=JDBC_PROPERTIES
        )

        # 2. Optimize & Count 
        df.cache() 
        row_count = df.count()

        # 3. Create External Table 
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {CONFIG["target_table"]}
            USING DELTA
            LOCATION '{CONFIG["target_path"]}'
        """)

        # 4. Write Data
        (df.write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .save(CONFIG["target_path"]))
         
        # Clear cache to free up cluster memory
        df.unpersist()

        # 5. Log Success
        log_audit_event("SUCCESS", start_ts=start_time, end_ts=get_utc_now(), row_count=row_count)
        print(f" Successfully loaded {row_count} rows from Azure SQL into {CONFIG['target_table']}.")

    except Exception as e:
        # 6. Log Failure & bubble up to Orchestrator
        error_msg = str(e)[:1000] # Truncate to fit schema limits
        log_audit_event("FAILED", start_ts=start_time, end_ts=get_utc_now(), error=error_msg)
        print(f" Pipeline Failed: {error_msg}")
        raise e # Re-raise for ADF to catch

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Execution Trigger
# MAGIC Standard Python entry point to trigger the ETL process.

# COMMAND ----------

if __name__ == "__main__":
    load_bronze_sql_layer()