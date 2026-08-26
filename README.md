# atliq-commerce-data-pipeline
**AtliQ Commerce End-to-End Data Engineering Project** — An automated Azure data pipeline using ADF, Databricks, Delta Lake, dbt, and Microsoft Fabric to ingest, transform, model, and visualize retail data.

# AtliQ Commerce — End-to-End Data Engineering Pipeline

An end-to-end data engineering project that syncs data from a live operational (OLTP) database into an analytics warehouse (OLAP), every night, and serves it through a Power BI dashboard in Microsoft Fabric — without ever touching the live application database for reporting.

The pipeline follows a **Bronze → Silver → Gold** medallion architecture:

```
SOURCES                 INGEST (ADF)         TRANSFORM (Databricks)        SERVE
─────────────           ────────────         ───────────────────────      ─────────
Azure SQL (OLTP)                              Bronze (raw Parquet)
  customers      ─┐                             │
  products        │     ┌──────────────┐        ▼
  orders          ├────▶│ Metadata-    │──▶  Silver (Delta, MERGE/UC)
  order_items     │     │ driven copy  │        │
  payments       ─┘     └──────────────┘        ▼
                                             Gold (star schema, dbt)   ──▶  Microsoft Fabric
CSV files       ─┐                                                          (Power BI)
  supplier price │
  marketing spend┘

Incremental sync every night: OLTP ──▶ Bronze ──▶ Silver ──▶ Gold ──▶ Fabric

Wrapped in Git + CI/CD + audit logging + data-quality tests
```

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Data Model](#data-model)
- [Implementation Walkthrough](#implementation-walkthrough)
  - [1. OLTP Setup](#1-oltp-setup)
  - [2. Ingestion — Bronze Layer (Azure Data Factory)](#2-ingestion--bronze-layer-azure-data-factory)
  - [3. Transformation — Silver Layer (Databricks)](#3-transformation--silver-layer-databricks)
  - [4. Modeling — Gold Layer (dbt Core)](#4-modeling--gold-layer-dbt-core)
  - [5. Orchestration](#5-orchestration)
  - [6. Audit & Monitoring](#6-audit--monitoring)
  - [7. Reporting (Microsoft Fabric)](#7-reporting-microsoft-fabric)
  - [8. CI/CD](#8-cicd)
- [Idempotency](#idempotency)
- [Repository Structure](#repository-structure)

## Architecture Overview

The core problem: running analytics queries directly against the live OLTP database slows down the application. This project solves it by maintaining two databases — a normalized OLTP store for transactions and a denormalized OLAP star schema for reporting — kept in sync by an automated nightly pipeline.

Data flows through three progressively cleaner layers:

- **Bronze** — raw, append-only landing zone in ADLS (Parquet), a faithful copy of the source
- **Silver** — cleaned, de-duplicated, governed Delta tables in Unity Catalog
- **Gold** — a denormalized star schema (fact + dimension tables) built with dbt, ready for reporting

## Tech Stack

| Layer | Tool |
|---|---|
| OLTP database | Azure SQL Database |
| File storage | Azure Data Lake Storage Gen2 (ADLS) |
| Ingestion | Azure Data Factory (ADF) |
| Transformation | Azure Databricks (PySpark + Delta Lake, Unity Catalog) |
| Modeling | dbt Core (running on Databricks) |
| Reporting | Microsoft Fabric (Power BI) |
| Version control / CI | Git + GitHub (GitHub Actions) |

## Data Model

**OLTP (3NF, built for writing)** — five normalized tables: `customers`, `products`, `orders`, `order_items`, `payments`. Each carries a watermark column (`updated_at` or `created_at` for insert-only `order_items`) that drives incremental extraction.

**OLAP (star schema, built for reading)**:

```
              dim_date
                 │
dim_customer ── fact_sales ── dim_product
```

| Table | Grain / role |
|---|---|
| `fact_sales` | One row per order item — `quantity`, `gross_revenue`, `item_price` |
| `dim_customer` | One row per customer — name, city, signup cohort |
| `dim_product` | One row per product — name, category, unit price, supplier cost, unit margin |
| `dim_date` | One row per calendar day — day, month, quarter, year, weekday |

## Implementation Walkthrough

### 1. OLTP Setup

- Provisioned an **Azure SQL Database** and ran the schema and seed scripts in order to create and populate the five OLTP tables (`customers`, `products`, `orders`, `order_items`, `payments`), plus the **ETL control table** that drives ingestion.
- Provisioned an **ADLS Gen2** storage account with a source directory to hold the raw CSV inputs (supplier price list, marketing spend), alongside the SQL sources.

### 2. Ingestion — Bronze Layer (Azure Data Factory)

Built a single **generic, metadata-driven** ADF pipeline rather than one pipeline per table:

- **Linked Services** were created to Azure SQL, ADLS, and Databricks.
- A **Lookup** activity reads the ETL control table (watermark, table name, load type) to drive SQL extraction. A **ForEach** loop iterates over each table and branches by `load_type`:
  - **Incremental** tables are filtered by their watermark column and land in a dated `ingest_date=` folder in Bronze.
  - **Full** tables are reloaded completely each run.
- A separate **metadata (Get Metadata) activity** scans the storage source directory, filters for CSV files, and copies them into their respective Bronze target locations.
- **Retry-safe writes:** before every copy — for both full-load SQL tables and the CSV files, as well as for the specific incremental date partition — the target location is deleted first, then the copy runs. This means a retried run overwrites cleanly instead of appending duplicate data, for both full and incremental loads.
- Bronze remains raw, append-only Parquet — no cleaning or de-duplication happens at this stage.

### 3. Transformation — Silver Layer (Databricks)

Once ingestion completes, ADF triggers a **Databricks job** that transforms Bronze into Silver:

- **Full tables** (`customers`, `products`, CSV sources) are read fresh from their latest Bronze snapshot, cleaned, and written with an **overwrite** into Silver Delta tables in Unity Catalog.
- **Incremental tables** (`orders`, `order_items`, `payments`) are read from the current run's dated Bronze batch, collapsed to the latest version per business key, and **MERGE**d (upserted) into their Silver Delta tables. This is what keeps the load idempotent — re-running the same batch never produces duplicates.
- Silver tables are the governed, query-ready source of truth in Unity Catalog.

### 4. Modeling — Gold Layer (dbt Core)

A **dbt task**, chained after the Silver transformation within the same Databricks job, builds the Gold star schema:

- **Staging models** read from the Silver tables (declared as dbt `sources`) — thin, one-to-one models that avoid hardcoding table names anywhere downstream.
- **Mart models** (`fact_sales`, `dim_customer`, `dim_product`, `dim_date`) are built on top of staging and materialized as external Delta tables at a known ADLS path, so Fabric can read them directly.
- dbt **tests** (uniqueness, not-null, referential integrity between fact and dimension keys) act as data-quality gates — bad data fails the build rather than reaching the dashboard.

### 5. Orchestration

The Databricks job (Silver transformation + dbt Gold build, as chained tasks within one job) is triggered directly from the ADF pipeline once ingestion succeeds. The full chain — ADF ingest → Databricks Silver → dbt Gold — runs as a single nightly, end-to-end sequence.

### 6. Audit & Monitoring

A three-tier audit design tracks every run end to end:

| Table | Tracks |
|---|---|
| **Pipeline run audit** | One row per pipeline execution — the parent record, keyed by `pipeline_id` |
| **ADF activity audit** | Every ADF activity's success/failure for a given `pipeline_id` |
| **Databricks job audit** | Every Databricks job task's (Silver + dbt) success/failure for the same `pipeline_id` |

Both the ADF activity audit and the Databricks job audit tables reference the parent pipeline run via `pipeline_id`, giving full lineage from a single nightly run down to the specific step that succeeded or failed.

An **email alert** is configured on the Databricks job so that if any task fails, a failure notification is sent directly to the project owner's email, rather than relying on someone noticing a stale dashboard the next morning.

### 7. Reporting (Microsoft Fabric)

- Gold tables are exposed to Fabric via a **OneLake shortcut** pointing at the Gold ADLS location — no data copy required.
- A **semantic model** was built on top of the shortcut, with relationships from `fact_sales` to each dimension table.
- A **Power BI report** was built on the semantic model to answer the core business questions (revenue trend, top products, top cities, new vs. returning customers).

### 8. CI/CD

- The dbt project's connection details are read from environment variables (no secrets committed), sourced from **GitHub Secrets** in CI.
- A **GitHub Actions workflow** validates the dbt project (`dbt build`) automatically whenever a pull request is raised from a feature branch, before code merges into main.

## Idempotency

The pipeline is designed to produce identical results no matter how many times it runs for the same period:

- **Bronze:** target locations (full-load tables, CSVs, and the specific incremental date partition) are deleted before every write, so a retry overwrites instead of appending duplicates.
- **Silver:** incremental tables are upserted via Delta `MERGE` on their business key; full tables are overwritten wholesale.
- **Gold:** dbt models are rebuilt cleanly from Silver on each run.

Running the nightly job twice in a row produces the same row counts and the same `gross_revenue` totals.

## Timestamp Consistency

Since timestamps are generated and compared across several different systems — Azure SQL, ADF, Databricks, and dbt — all watermark and audit timestamps are standardized to **UTC** throughout the pipeline. This avoids subtle bugs where a local time zone offset causes rows to be skipped or reprocessed incorrectly during the incremental watermark comparison.

## Repository Structure

The project is organized into folders by layer/responsibility, matching the architecture described above:

```
.
├── atliq_commerce_adf/          # Ingestion — ADF pipeline, linked services, datasets,
│                                 # and the metadata-driven Bronze layer setup
├── databricks_silver_transform/  # Databricks job — Silver layer PySpark transformations,
│                                 # plus the dbt task that builds Gold, chained in the same job
├── atliq_dbt_gold/                # dbt project — staging models (reading Silver) and
│                                 # Gold mart models (fact_sales, dim_customer, dim_product,
│                                 # dim_date), tests, and profiles
├── fabric_analytics/             # Microsoft Fabric — OneLake shortcut to the Gold external
│                                 # location, semantic model, and the Power BI report
├── audit/                        # DDL and scripts for the three audit tables (pipeline run,
│                                 # ADF activity, Databricks job) used for monitoring
├── streaming_pipeline_kafka/      # Separate, minor side project — a Kafka-based streaming
│                                 # pipeline, built independently of the main nightly batch flow
├── .github/
│   └── workflows/                # CI workflow (dbt build validation on pull request)
└── README.md
```

| Folder | Purpose |
|---|---|
| `atliq_commerce_adf` | Ingestion layer — the generic, metadata-driven ADF pipeline that lands source data into Bronze |
| `databricks_silver_transform` | Transformation layer — Bronze to Silver PySpark notebooks, with the dbt Gold build chained in as a task in the same job |
| `atliq_dbt_gold` | Modeling layer — the dbt Core project that builds the Gold star schema from Silver |
| `fabric_analytics` | Reporting layer — the Fabric shortcut, semantic model, and Power BI report built on Gold |
| `audit` | Audit layer — DDL and supporting scripts for the pipeline run, ADF activity, and Databricks job audit tables |
| `streaming_pipeline_kafka` | A separate, minor Kafka-based streaming pipeline project, built independently of the main nightly batch pipeline |
