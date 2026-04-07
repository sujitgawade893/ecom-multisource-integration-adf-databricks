# E-Commerce Multi‑Source Data Integration (ADLS File + Azure SQL → Databricks UC → ADF → Power BI)
<img width="940" height="433" alt="image" src="https://github.com/user-attachments/assets/425ab6c0-9d80-4b3d-86b2-8819ac7190b5" />

## Overview
This project implements an end‑to‑end multi‑source data integration pipeline using **Azure Databricks (Unity Catalog)** and **Azure Data Factory (ADF)**.  
Two sources are integrated:
<img width="940" height="433" alt="image" src="https://github.com/user-attachments/assets/c49061e0-9784-4b81-8c86-57f3a494917a" />

- **Source 1 (File / SFTP simulation):** E‑commerce orders dataset as a CSV landed in **ADLS Gen2**
  <img width="940" height="436" alt="image" src="https://github.com/user-attachments/assets/66db1432-be85-4e5f-a4fd-7160a4a3cacd" />

- **Source 2 (Azure SQL Database):** `customers` table (subset of columns)

The pipeline lands data into a **Bronze → Silver → Gold** architecture, builds a **unified reporting table**, logs each pipeline step to an **audit table**, and supports dashboarding in **Power BI**.

---

## Tech Stack
- Azure Databricks (Spark, PySpark, Delta Lake)
- Unity Catalog (External Locations, governed access)
- ADLS Gen2 (raw/processed storage)
- Azure SQL Database (customers dimension table)
- Azure Data Factory (pipeline orchestration)
- Power BI Desktop (dashboard)

---

## Architecture (High level)
**ADF Orchestration**
1. Notebook 01: File → Bronze (Delta)
2. Notebook 02: Azure SQL → Bronze (Delta)
3. Notebook 03: Silver cleaning + dedupe + Gold unified reporting

**Medallion Layers**
- **Bronze:** raw landed tables (Delta)
- **Silver:** cleaned & standardized + deduplicated entities
- **Gold:** analytics‑ready unified reporting table
<img width="1205" height="622" alt="image" src="https://github.com/user-attachments/assets/9b2c3b73-01e0-4bef-914f-5f4f1dcba545" />


---

## Data Sources

### Source 1: Orders (CSV in ADLS)
File path (example):
- `abfss://project@ecomstora.dfs.core.windows.net/rawdata/Ecommerce_2000.csv`
- <img width="940" height="434" alt="image" src="https://github.com/user-attachments/assets/b128b279-ca23-4ce7-9832-9169db3db221" />



### Source 2: Customers (Azure SQL)
Table:
```sql
CREATE TABLE dbo.customers (
  customer_id   VARCHAR(10),
  customer_name VARCHAR(100),
  city          VARCHAR(50)
);
 ```
<img width="940" height="433" alt="image" src="https://github.com/user-attachments/assets/0137f8af-8697-478c-9f7a-0e247e241adf" />


### Customers are populated from the same dataset by selecting:

CustomerID → customer_id
FirstName + LastName → customer_name
City → city
Unity Catalog Objects

### External Locations:
<img width="940" height="433" alt="image" src="https://github.com/user-attachments/assets/ac50a33f-ff3e-4b08-a156-f408186caea3" />

rawdata → ADLS raw container/folder
processed → ADLS processed container/folder (Delta tables & audit logs)
Audit logging table (external Delta):
  ecomwork.audit.pipeline_log

### Output Tables (Delta)
Bronze
ecomwork.bronze.orders_file_raw
ecomwork.bronze.customers_sql_raw
Silver
ecomwork.silver.orders (cleaned + deduplicated by OrderID if required)
ecomwork.silver.customers (cleaned + deduplicated by customer_id)
Gold
ecomwork.gold.unified_reporting
Grain: order-level (one row per order after dedupe), enriched with SQL customer attributes.

### Notebook Run Order (ADF)
<img width="940" height="435" alt="image" src="https://github.com/user-attachments/assets/846a978d-8076-4ba7-9fdc-6ab60bd5aa39" />

01_bronze_orders_file
02_bronze_customers_sql_raw
03_silver_gold_merge
Each notebook accepts ADF parameters:

pipeline_name
run_id (@pipeline().RunId)

### Audit / Logging (Observability)
Each pipeline step writes to:

ecomwork.audit.pipeline_log
Logged fields include:

pipeline_name, run_id, step_name
status (STARTED / SUCCESS / FAILED)
start_ts, end_ts
row_count
error_message (if failed)
This enables traceability per ADF pipeline run.

### How to Run (High level)
Configure Unity Catalog external locations and permissions.
Ensure Azure SQL dbo.customers exists and is accessible.
Run ADF pipeline (or run notebooks manually in order).
Validate Gold table and audit logs.
Validation Queries
SQL

-- Gold output
SELECT COUNT(*) FROM ecomwork.gold.unified_reporting;
SELECT * FROM ecomwork.gold.unified_reporting LIMIT 10;

-- Audit logs (filter by ADF run_id)
SELECT *
FROM ecomwork.audit.pipeline_log
ORDER BY start_ts DESC;
Power BI (Offline export approach)
If Databricks SQL Warehouse connectivity is not used, the Gold table is exported to CSV from Databricks and loaded into Power BI Desktop for reporting.

(Attach Power BI dashboard screenshots later.)
