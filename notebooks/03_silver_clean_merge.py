# Databricks notebook source
# MAGIC %md
# MAGIC # **Silver & Gold Processing (Bronze → Silver → Gold)
# MAGIC
# MAGIC Reads Bronze tables from File and Azure SQL
# MAGIC Cleans + deduplicates in Silver
# MAGIC Joins to create Gold unified reporting table
# MAGIC Writes audit logs to ecomwork.audit.pipeline_log**

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
from pyspark.sql.functions import col, trim, upper, lower, to_date, when, first

def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)

# ADF parameters
dbutils.widgets.text("pipeline_name", "ecom_pipeline")
dbutils.widgets.text("run_id", "")

pipeline_name = dbutils.widgets.get("pipeline_name")
run_id_widget = dbutils.widgets.get("run_id")
run_id = run_id_widget if run_id_widget else str(int(utc_now().timestamp()))

LOG_TABLE = "ecomwork.audit.pipeline_log"

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

def log(step, status, start_ts, end_ts=None, source=None, target=None, row_count=None, error=None):
    data = [(pipeline_name, run_id, step, status, start_ts, end_ts, source, target,
             None if row_count is None else int(row_count), error)]
    (spark.createDataFrame(data, schema=LOG_SCHEMA)
         .write.mode("append")
         .saveAsTable(LOG_TABLE))

def write_external_delta_table(df, table_name, location_path):
    # Create table metadata if not exists
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {table_name}
      USING DELTA
      LOCATION '{location_path}'
    """)
    # Overwrite data at the path
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(location_path))

# COMMAND ----------

# MAGIC %md
# MAGIC **Outputs**

# COMMAND ----------

# Bronze inputs
ORDERS_BRONZE_TBL = "ecomwork.bronze.orders_file_raw"
CUSTOMERS_BRONZE_TBL = "ecomwork.bronze.customers_sql_raw"

# Silver outputs
ORDERS_SILVER_TBL = "ecomwork.silver.orders"
CUSTOMERS_SILVER_TBL = "ecomwork.silver.customers"

# Gold output
GOLD_TBL = "ecomwork.gold.unified_reporting"

# External Delta locations (processed)
ORDERS_SILVER_PATH = "abfss://project@ecomstora.dfs.core.windows.net/processed/silver/orders"
CUSTOMERS_SILVER_PATH = "abfss://project@ecomstora.dfs.core.windows.net/processed/silver/customers"
GOLD_PATH = "abfss://project@ecomstora.dfs.core.windows.net/processed/gold/unified_reporting"

# COMMAND ----------

# MAGIC %md
# MAGIC ** Read Source 1 (Bronze Orders from File)**

# COMMAND ----------

step = "read_bronze_orders"
start = utc_now()
log(step, "STARTED", start_ts=start, source=ORDERS_BRONZE_TBL, target="df_orders_bronze")

try:
    df_orders_bronze = spark.table(ORDERS_BRONZE_TBL)
    log(step, "SUCCESS", start_ts=start, end_ts=utc_now(), source=ORDERS_BRONZE_TBL, target="df_orders_bronze",
        row_count=df_orders_bronze.count())
except Exception as e:
    log(step, "FAILED", start_ts=start, end_ts=utc_now(), source=ORDERS_BRONZE_TBL, target="df_orders_bronze", error=str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC **Read Source 2 (Bronze Customers from Azure SQL landing)**

# COMMAND ----------

step = "read_bronze_customers"
start = utc_now()
log(step, "STARTED", start_ts=start, source=CUSTOMERS_BRONZE_TBL, target="df_customers_bronze")

try:
    df_customers_bronze = spark.table(CUSTOMERS_BRONZE_TBL)
    log(step, "SUCCESS", start_ts=start, end_ts=utc_now(), source=CUSTOMERS_BRONZE_TBL, target="df_customers_bronze",
        row_count=df_customers_bronze.count())
except Exception as e:
    log(step, "FAILED", start_ts=start, end_ts=utc_now(), source=CUSTOMERS_BRONZE_TBL, target="df_customers_bronze", error=str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC **Quick Preview**

# COMMAND ----------

display(df_orders_bronze.limit(5))
display(df_customers_bronze.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC # ## # **Silver Transform**

# COMMAND ----------

step = "silver_orders_clean"
start = utc_now()
log(step, "STARTED", start_ts=start, source=ORDERS_BRONZE_TBL, target=ORDERS_SILVER_TBL)

try:
    df_orders_silver = (
        df_orders_bronze
        .withColumn("OrderID", trim(col("OrderID")))
        .withColumn("CustomerID", trim(col("CustomerID")))
        .withColumn("City", trim(col("City")))
        .withColumn("State", upper(trim(col("State"))))
        .withColumn("OrderDate", to_date(col("OrderDate"), "M/d/yyyy"))
        .withColumn("Returned", lower(trim(col("Returned"))))
        .withColumn(
            "Returned",
            when(col("Returned").isin("yes","y","true","1"), "yes")
            .when(col("Returned").isin("no","n","false","0"), "no")
            .otherwise(col("Returned"))
        )
    )

    # --- DEDUPE orders by OrderID (keep latest OrderDate) ---
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    w = Window.partitionBy("OrderID").orderBy(col("OrderDate").desc_nulls_last())

    df_orders_silver_dedup = (
        df_orders_silver
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    df_orders_silver_dedup = df_orders_silver_dedup.cache()
    rc = df_orders_silver_dedup.count()

    # write the deduped DF to Silver
    write_external_delta_table(df_orders_silver_dedup, ORDERS_SILVER_TBL, ORDERS_SILVER_PATH)

    log(step, "SUCCESS", start_ts=start, end_ts=utc_now(),
        source=ORDERS_BRONZE_TBL, target=ORDERS_SILVER_TBL, row_count=rc)

except Exception as e:
    log(step, "FAILED", start_ts=start, end_ts=utc_now(),
        source=ORDERS_BRONZE_TBL, target=ORDERS_SILVER_TBL, error=str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC **Silver Transform: Clean + Deduplicate Customers **

# COMMAND ----------

step = "silver_customers_clean_dedupe"
start = utc_now()
log(step, "STARTED", start_ts=start, source=CUSTOMERS_BRONZE_TBL, target=CUSTOMERS_SILVER_TBL)

try:
    customers_clean = (
        df_customers_bronze
        .select(
            trim(col("customer_id")).alias("customer_id"),
            trim(col("customer_name")).alias("customer_name"),
            trim(col("city")).alias("city")
        )
        .filter(col("customer_id").isNotNull() & (col("customer_id") != ""))
    )

    # Dedupe in Silver 
    df_customers_silver = (
        customers_clean
        .groupBy("customer_id")
        .agg(
            first("customer_name", ignorenulls=True).alias("customer_name"),
            first("city", ignorenulls=True).alias("city")
        )
    )

    df_customers_silver = df_customers_silver.cache()
    rc = df_customers_silver.count()

    log(step, "SUCCESS", start_ts=start, end_ts=utc_now(), source=CUSTOMERS_BRONZE_TBL, target=CUSTOMERS_SILVER_TBL, row_count=rc)

except Exception as e:
    log(step, "FAILED", start_ts=start, end_ts=utc_now(), source=CUSTOMERS_BRONZE_TBL, target=CUSTOMERS_SILVER_TBL, error=str(e))
    raise

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, COUNT(*) cnt
# MAGIC FROM ecomwork.silver.customers
# MAGIC GROUP BY customer_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ** Write Silver Tables**

# COMMAND ----------

step = "write_silver_tables"
start = utc_now()
log(step, "STARTED", start_ts=start, source="dataframes", target="silver paths")

try:
    write_external_delta_table(df_orders_silver_dedup, ORDERS_SILVER_TBL, ORDERS_SILVER_PATH)
    write_external_delta_table(df_customers_silver, CUSTOMERS_SILVER_TBL, CUSTOMERS_SILVER_PATH)

    log(step, "SUCCESS", start_ts=start, end_ts=utc_now(), source="dataframes", target="silver paths")
except Exception as e:
    log(step, "FAILED", start_ts=start, end_ts=utc_now(), source="dataframes", target="silver paths", error=str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Gold Join + Write Unified Reporting Table**

# COMMAND ----------

step = "gold_unified_reporting"
start = utc_now()
log(step, "STARTED", start_ts=start, source=f"{ORDERS_SILVER_TBL} + {CUSTOMERS_SILVER_TBL}", target=GOLD_TBL)

try:
    unified = (
        df_orders_silver_dedup.alias("o")          # deduped orders
        .join(
            df_customers_silver.alias("c"),        # deduped customers
            col("o.CustomerID") == col("c.customer_id"),
            "left"
        )
        .select(
            col("o.*"),
            col("c.customer_name").alias("sql_customer_name"),
            col("c.city").alias("sql_city")
        )
    )

    unified = unified.cache()
    rc = unified.count()

    write_external_delta_table(unified, GOLD_TBL, GOLD_PATH)

    log(step, "SUCCESS", start_ts=start, end_ts=utc_now(),
        source=f"{ORDERS_SILVER_TBL} + {CUSTOMERS_SILVER_TBL}", target=GOLD_TBL, row_count=rc)

except Exception as e:
    log(step, "FAILED", start_ts=start, end_ts=utc_now(),
        source=f"{ORDERS_SILVER_TBL} + {CUSTOMERS_SILVER_TBL}", target=GOLD_TBL, error=str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Validation Queries**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM ecomwork.silver.orders;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM ecomwork.silver.customers;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM ecomwork.gold.unified_reporting;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS dup_order_ids
# MAGIC FROM (
# MAGIC   SELECT OrderID
# MAGIC   FROM ecomwork.gold.unified_reporting
# MAGIC   GROUP BY OrderID
# MAGIC   HAVING COUNT(*) > 1
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM ecomwork.silver.orders;
# MAGIC SELECT COUNT(*) FROM ecomwork.silver.customers;
# MAGIC SELECT COUNT(*) FROM ecomwork.gold.unified_reporting;
# MAGIC
# MAGIC SELECT * FROM ecomwork.audit.pipeline_log
# MAGIC ORDER BY start_ts DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, COUNT(*) cnt
# MAGIC FROM ecomwork.silver.customers
# MAGIC GROUP BY customer_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS null_customerid_rows
# MAGIC FROM ecomwork.gold.unified_reporting
# MAGIC WHERE CustomerID IS NULL OR trim(CustomerID) = '';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS unmatched_customer_rows
# MAGIC FROM ecomwork.gold.unified_reporting
# MAGIC WHERE CustomerID IS NOT NULL
# MAGIC   AND trim(CustomerID) <> ''
# MAGIC   AND sql_customer_name IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   SUM(CASE WHEN CustomerID IS NULL OR trim(CustomerID) = '' THEN 1 ELSE 0 END) AS null_customerid,
# MAGIC   SUM(CASE WHEN CustomerID IS NOT NULL AND trim(CustomerID) <> '' AND sql_customer_name IS NULL THEN 1 ELSE 0 END) AS unmatched_customerid
# MAGIC FROM ecomwork.gold.unified_reporting;