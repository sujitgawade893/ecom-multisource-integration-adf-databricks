# Databricks notebook source
# MAGIC %sql
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION processed TO `sujit08092003@gmail.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ecomwork.audit;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecomwork.audit.pipeline_log (
# MAGIC   pipeline_name STRING,
# MAGIC   run_id STRING,
# MAGIC   step_name STRING,
# MAGIC   status STRING,
# MAGIC   start_ts TIMESTAMP,
# MAGIC   end_ts TIMESTAMP,
# MAGIC   source STRING,
# MAGIC   target STRING,
# MAGIC   row_count BIGINT,
# MAGIC   error_message STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://project@ecomstora.dfs.core.windows.net/processed/audit/pipeline_log';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ecomwork.audit.pipeline_log ORDER BY start_ts DESC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ecomwork;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS ecomwork.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecomwork.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecomwork.gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecomwork.audit;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecomwork.audit.pipeline_log (
# MAGIC   pipeline_name STRING,
# MAGIC   run_id        STRING,
# MAGIC   step_name     STRING,
# MAGIC   status        STRING,          -- STARTED / SUCCESS / FAILED
# MAGIC   start_ts      TIMESTAMP,
# MAGIC   end_ts        TIMESTAMP,
# MAGIC   source        STRING,
# MAGIC   target        STRING,
# MAGIC   row_count     BIGINT,
# MAGIC   error_message STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC