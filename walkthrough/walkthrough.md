# Atliq Commerce Data Pipeline — Execution Walkthrough

This walkthrough uses execution screenshots to document the end-to-end pipeline from ingestion through transformation, validation, and the Gold analytical model.

## 1. ADF activity-run view

![ADF activity-run view showing the ingestion pipeline completed successfully across its metadata, control-table, copy, and Databricks activities.](images/01_adf_activity_runs.png)

> ADF activity-run view showing the ingestion pipeline completed successfully across its metadata, control-table, copy, and Databricks activities.

## 2. Kafka lineage

![Kafka lineage showing the order-event producer publishing to `atliq.orders.events`, which is consumed by the Spark Kafka consumer.](images/02_kafka_lineage.png)

> Kafka lineage showing the order-event producer publishing to `atliq.orders.events`, which is consumed by the Spark Kafka consumer.

## 3. Pipeline audit output capturing the pipeline run ID, run date, pipeline name, and execution timestamp for traceability.

![Pipeline audit output capturing the pipeline run ID, run date, pipeline name, and execution timestamp for traceability.](images/03_pipeline_audit.png)

> Pipeline audit output capturing the pipeline run ID, run date, pipeline name, and execution timestamp for traceability.

## 4. Gold `dim_products` query result

![Gold `dim_products` query result showing product attributes, pricing, supplier cost, and calculated unit margin.](images/04_gold_dim_products.png)

> Gold `dim_products` query result showing product attributes, pricing, supplier cost, and calculated unit margin.

## 5. Control table defining full versus incremental loading and the watermark column used for each source table.

![Control table defining full versus incremental loading and the watermark column used for each source table.](images/05_control_table.png)

> Control table defining full versus incremental loading and the watermark column used for each source table.

## 6. Gold `dim_date` query result

![Gold `dim_date` query result showing calendar attributes such as year, quarter, month, week, and day.](images/06_gold_dim_date.png)

> Gold `dim_date` query result showing calendar attributes such as year, quarter, month, week, and day.

## 7. ADF pipeline design

![ADF pipeline design showing metadata discovery, control-table processing, CSV handling, and the Databricks job invocation.](images/07_adf_pipeline_design.png)

> ADF pipeline design showing metadata discovery, control-table processing, CSV handling, and the Databricks job invocation.

## 8. ADF Trigger Runs view confirming that the `atliq_trigger` schedule trigger executed successfully.

![ADF Trigger Runs view confirming that the `atliq_trigger` schedule trigger executed successfully.](images/08_trigger_run.png)

> ADF Trigger Runs view confirming that the `atliq_trigger` schedule trigger executed successfully.

## 9. Detailed ADF pipeline flow

![Detailed ADF pipeline flow showing the full-load/incremental-load decision path and CSV processing before Databricks execution.](images/09_adf_pipeline_overview.png)

> Detailed ADF pipeline flow showing the full-load/incremental-load decision path and CSV processing before Databricks execution.

## 10. Schedule-trigger configuration

![Schedule-trigger configuration showing daily execution at 9:00 PM in the India time zone.](images/10_trigger_configuration.png)

> Schedule-trigger configuration showing daily execution at 9:00 PM in the India time zone.

## 11. Gold dimensional model

![Gold dimensional model showing `fact_sales` connected to the customer, product, and date dimensions.](images/11_gold_model_relationships.png)

> Gold dimensional model showing `fact_sales` connected to the customer, product, and date dimensions.

## 12. Gold `dim_customers` query result

![Gold `dim_customers` query result showing customer keys and descriptive customer attributes.](images/12_gold_dim_customers.png)

> Gold `dim_customers` query result showing customer keys and descriptive customer attributes.

## 13. Bronze storage view

![Bronze storage view showing the order data partitioned by `ingest_date=2026-08-23`.](images/13_bronze_orders_partition.png)

> Bronze storage view showing the order data partitioned by `ingest_date=2026-08-23`.

## 14. Gold model view highlighting the star-schema relationships between the sales fact and its dimensions.

![Gold model view highlighting the star-schema relationships between the sales fact and its dimensions.](images/14_gold_model_relationships_2.png)

> Gold model view highlighting the star-schema relationships between the sales fact and its dimensions.

## 15. Bronze storage view listing the ingested source datasets such as customers, orders, products, payments, and supporting files.

![Bronze storage view listing the ingested source datasets such as customers, orders, products, payments, and supporting files.](images/15_bronze_sources.png)

> Bronze storage view listing the ingested source datasets such as customers, orders, products, payments, and supporting files.

## 16. Silver-layer validation query

![Silver-layer validation query showing row counts for customers, products, orders, order items, payments, marketing spend, and supplier price list.](images/16_silver_row_counts.png)

> Silver-layer validation query showing row counts for customers, products, orders, order items, payments, marketing spend, and supplier price list.

## 17. Databricks transformation job graph

![Databricks transformation job graph showing successful Silver transformations followed by the Gold dbt build.](images/17_databricks_job_graph.png)

> Databricks transformation job graph showing successful Silver transformations followed by the Gold dbt build.

## 18. Databricks job run view confirming successful execution of the transformation workflow.

![Databricks job run view confirming successful execution of the transformation workflow.](images/18_databricks_job_run.png)

> Databricks job run view confirming successful execution of the transformation workflow.

## 19. Compact Silver validation result confirming the processed record counts for the main transactional tables.

![Compact Silver validation result confirming the processed record counts for the main transactional tables.](images/19_silver_counts_summary.png)

> Compact Silver validation result confirming the processed record counts for the main transactional tables.

## 20. Additional ADF execution evidence confirming successful activity completion for the ingestion pipeline.

![Additional ADF execution evidence confirming successful activity completion for the ingestion pipeline.](images/20_adf_activity_runs_duplicate.png)

> Additional ADF execution evidence confirming successful activity completion for the ingestion pipeline.

## End-to-End Flow

The screenshots collectively demonstrate the flow:

**Source systems → Azure Data Factory → Bronze → Databricks/Silver → dbt Gold → Analytical model**

A separate streaming path is also shown:

**Kafka producer → `atliq.orders.events` → Spark Kafka consumer**
