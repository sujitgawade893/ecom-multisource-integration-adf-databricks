# E-Commerce Multi‑Source Data Integration (ADLS File + Azure SQL → Databricks UC → ADF → Power BI)

## Overview
This project implements an end‑to‑end multi‑source data integration pipeline using **Azure Databricks (Unity Catalog)** and **Azure Data Factory (ADF)**.  
Two sources are integrated:

- **Source 1 (File / SFTP simulation):** E‑commerce orders dataset as a CSV landed in **ADLS Gen2**
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


---

## Data Sources

### Source 1: Orders (CSV in ADLS)
File path (example):
- `abfss://project@ecomstora.dfs.core.windows.net/rawdata/Ecommerce_2000.csv`

### Source 2: Customers (Azure SQL)
Table:
```sql
CREATE TABLE dbo.customers (
  customer_id   VARCHAR(10),
  customer_name VARCHAR(100),
  city          VARCHAR(50)
);
