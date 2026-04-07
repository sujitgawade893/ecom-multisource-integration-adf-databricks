# Databricks notebook source
# MAGIC %md
# MAGIC # **Creating the external locations**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS rawdata
# MAGIC URL 'abfss://project@ecomstora.dfs.core.windows.net/rawdata/'
# MAGIC WITH (STORAGE CREDENTIAL `ecomcread`)
# MAGIC COMMENT 'Raw data external location';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS;

# COMMAND ----------

# MAGIC %md
# MAGIC #  Grant access to a group

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ FILES ON EXTERNAL LOCATION rawdata TO `xxxxxxxxxx@gmail.com`;
# MAGIC -- only if they should write/delete:
# MAGIC GRANT WRITE FILES ON EXTERNAL LOCATION rawdata TO `xxxxxxxxxx@gmail.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON EXTERNAL LOCATION rawdata;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating the catalog and schema**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ecomwork;
# MAGIC USE CATALOG ecomwork;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze schema
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC
# MAGIC -- CSV External Table (infers schema)
# MAGIC CREATE TABLE IF NOT EXISTS bronze.csv_raw_data
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = 'true',
# MAGIC   inferSchema = 'true',
# MAGIC   delimiter = ','
# MAGIC )
# MAGIC LOCATION 'abfss://project@ecomstora.dfs.core.windows.net/rawdata/csv_data/'; 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecomwork.bronze.csv_raw_data;