# Databricks notebook source
# MAGIC %md
# MAGIC # **Unified Gold Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN ecomwork.gold;
# MAGIC SELECT COUNT(*) FROM ecomwork.gold.unified_reporting;
# MAGIC SELECT * FROM ecomwork.gold.unified_reporting LIMIT 10;

# COMMAND ----------

df = spark.table("ecomwork.gold.unified_reporting")

out_dir = "abfss://project@ecomstora.dfs.core.windows.net/processed/powerbi/unified_reporting_csv"

(df.coalesce(1)   # makes 1 CSV part file (ok for small/medium data)
   .write.mode("overwrite")
   .option("header", "true")
   .csv(out_dir)) 

# COMMAND ----------

files = dbutils.fs.ls(out_dir)
files