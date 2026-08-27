"""Bronze-to-Silver incremental upsert pipeline for the orders table.

Reads a single Bronze ingestion-date partition, keeps the latest
version of each order, upserts into the Silver table, and logs an
audit record for the run (success or failure).
"""

import argparse
from datetime import datetime, timezone
from pyspark.sql import DataFrame, SparkSession, functions as F

from databricks_silver_transform.src.audit.audit_logger import write_audit_log
from databricks_silver_transform.src.common.logger import get_logger
from databricks_silver_transform.src.common.silver_utils import (
    dedupe_latest,
    get_silver_table,
    read_bronze_batch,
    upsert_to_silver,
)

logger = get_logger(__name__)

TABLE_NAME = "orders"
SILVER_TABLE = get_silver_table(TABLE_NAME)

def transform(spark: SparkSession, run_date: str) -> DataFrame:
    """
    Transform the Bronze orders batch into the Silver orders dataset.

    Transformations:
    - Read only the specified ingestion-date partition.
    - Keep the latest version of each order within the batch.
    - Convert order_date to DATE.
    - Cast order_amount to DECIMAL(12,2).
    - Remove records with null order_id.
    """

    logger.info("Reading Bronze batch for table=%s, run_date=%s", TABLE_NAME, run_date)

    batch_df = read_bronze_batch(spark=spark, adls_dir=TABLE_NAME, run_date=run_date)

    logger.info("Deduplicating latest records for table=%s", TABLE_NAME)

    orders_df = dedupe_latest(df=batch_df, key_col="order_id", order_col="updated_at")

    logger.info("Applying Silver transformations for table=%s", TABLE_NAME)

    return (
        orders_df
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("order_amount", F.col("order_amount").cast("decimal(12,2)"))
        .filter(F.col("order_id").isNotNull())
    )

def run(
    spark: SparkSession,
    run_date: str,
    pipeline_run_id: str,
    job_name: str,
    job_run_id: str,
    task_name: str,
    task_run_id: str,
) -> None:
    """
    Run the Bronze-to-Silver orders incremental upsert pipeline.

    Transforms the batch, upserts it into the Silver table on
    order_id (updating rows with a newer updated_at, inserting new
    ones), then writes a Success audit log with the combined
    insert+update count. On failure, writes a Fail audit log with
    the error message and re-raises.

    Args:
        spark: Active SparkSession.
        run_date: Bronze ingestion-date partition to process.
        pipeline_run_id: Orchestration-level run identifier.
        job_name: Databricks job name.
        job_run_id: Databricks job run identifier.
        task_name: Databricks task name.
        task_run_id: Databricks task run identifier.

    Raises:
        Exception: Re-raises any error from transform/upsert after
            logging it and recording a Fail audit entry.
    """

    start_time = datetime.now(timezone.utc)

    try:
        silver_orders_df = transform(spark=spark, run_date=run_date)

        before_count = spark.table(SILVER_TABLE).count()

        logger.info("Current Silver count before merge for table=%s: %s", TABLE_NAME, before_count)

        incoming_count = silver_orders_df.count()

        logger.info("Incoming batch records for table=%s: %s", TABLE_NAME, incoming_count)

        silver_orders_existing_df = spark.table(SILVER_TABLE)

        incoming_updates_df = (
            silver_orders_df.alias("source")
            .join(silver_orders_existing_df.alias("target"), F.col("source.order_id") == F.col("target.order_id"), "inner")
            .filter(F.col("source.updated_at") > F.col("target.updated_at"))
        )

        update_count = incoming_updates_df.count()

        incoming_inserts_df = (
            silver_orders_df.alias("source")
            .join(silver_orders_existing_df.alias("target"), F.col("source.order_id") == F.col("target.order_id"), "left_anti")
        )

        insert_count = incoming_inserts_df.count()

        logger.info(
            "Upsert summary for table=%s: "
            "incoming=%s, updates=%s, inserts=%s",
            TABLE_NAME,
            incoming_count,
            update_count,
            insert_count,
        )

        logger.info("Upserting records into Silver table=%s", SILVER_TABLE)

        upsert_to_silver(
            spark=spark,
            df=silver_orders_df,
            table_name=SILVER_TABLE,
            merge_key="target.order_id = source.order_id",
            update_condition=(
                "source.updated_at > target.updated_at"
            ),
        )

        after_count = spark.table(SILVER_TABLE).count()

        logger.info("Current Silver count after merge for table=%s: %s", TABLE_NAME, after_count)

        end_time = datetime.now(timezone.utc)

        logger.info(
            "Successfully completed Silver pipeline for table=%s, "
            "run_date=%s. Before=%s, Incoming=%s, Updates=%s, "
            "Inserts=%s, After=%s",
            TABLE_NAME,
            run_date,
            before_count,
            incoming_count,
            update_count,
            insert_count,
            after_count,
        )

        write_audit_log(
            spark=spark,
            run_date=run_date,
            pipeline_run_id=pipeline_run_id,
            job_name=job_name,
            job_run_id=job_run_id,
            task_name=task_name,
            task_run_id=task_run_id,
            layer="Silver",
            source_name=SILVER_TABLE,
            load_type="Incremental",
            status="Success",
            row_count=insert_count + update_count,
            error_message=None,
            start_time=start_time,
            end_time=end_time,
        )

    except Exception as e:

        end_time = datetime.now(timezone.utc)

        logger.exception("Silver pipeline failed for table=%s, run_date=%s", TABLE_NAME, run_date)

        try:

            write_audit_log(
                spark=spark,
                run_date=run_date,
                pipeline_run_id=pipeline_run_id,
                job_name=job_name,
                job_run_id=job_run_id,
                task_name=task_name,
                task_run_id=task_run_id,
                layer="Silver",
                source_name=SILVER_TABLE,
                load_type="Incremental",
                status="Fail",
                row_count=None,
                error_message=str(e)[:4000],
                start_time=start_time,
                end_time=end_time,
            )

        except Exception:
            logger.exception("Failed to write audit record for table=%s", TABLE_NAME)

        # Preserve the original pipeline failure.
        raise

def parse_args() -> argparse.Namespace:
    """Parse Databricks Job parameters."""

    parser = argparse.ArgumentParser(
        description="Silver orders pipeline",
    )

    parser.add_argument("--run_date", required=True)
    parser.add_argument("--pipeline_run_id",required=True)
    parser.add_argument("--job_name", required=True)
    parser.add_argument("--job_run_id", required=True)
    parser.add_argument("--task_name", required=True)
    parser.add_argument("--task_run_id", required=True)


    return parser.parse_args()

def main() -> None:
    """Application entry point."""

    args = parse_args()

    spark = SparkSession.builder.appName(args.task_name).getOrCreate()

    run(
        spark=spark,
        run_date=args.run_date,
        pipeline_run_id=args.pipeline_run_id,
        job_name=args.job_name,
        job_run_id=args.job_run_id,
        task_name=args.task_name,
        task_run_id=args.task_run_id,
    )


if __name__ == "__main__":
    main()