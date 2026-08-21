import argparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, functions as F

from databricks_jobs.src.audit.audit_logger import write_audit_log
from databricks_jobs.src.common.logger import get_logger
from databricks_jobs.src.common.silver_utils import (
    get_silver_table,
    insert_to_silver,
    read_bronze_batch,
)


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

TABLE_NAME = "order_items"
SILVER_TABLE = get_silver_table(TABLE_NAME)

logger = get_logger(__name__)


# ---------------------------------------------------------
# Transformation
# ---------------------------------------------------------

def transform(
    spark: SparkSession,
    run_date: str,
) -> DataFrame:
    """
    Transform the Bronze order_items batch into the Silver dataset.

    Transformations:
    - Read only the specified ingestion-date partition.
    - Deduplicate the current batch by order_item_id.
    - Remove records with null order_item_id.
    - Remove records with null order_id.
    - Remove records with null product_id.
    - Keep only records with positive quantity.
    - Keep only records with non-negative item_price.
    """

    logger.info(
        "Reading Bronze batch for table=%s, run_date=%s",
        TABLE_NAME,
        run_date,
    )

    batch_df = read_bronze_batch(
        spark=spark,
        table_name=TABLE_NAME,
        run_date=run_date,
    )

    logger.info(
        "Applying Silver transformations for table=%s",
        TABLE_NAME,
    )

    return (
        batch_df
        .dropDuplicates(["order_item_id"])
        .filter(
            F.col("order_item_id").isNotNull()
        )
        .filter(
            F.col("order_id").isNotNull()
        )
        .filter(
            F.col("product_id").isNotNull()
        )
        .filter(
            F.col("quantity") > 0
        )
        .filter(
            F.col("item_price") >= 0
        )
    )


# ---------------------------------------------------------
# Silver Pipeline
# ---------------------------------------------------------

def run(
    spark: SparkSession,
    run_date: str,
    pipeline_run_id: str,
    pipeline_name: str,
    job_name: str,
    job_run_id: str,
    task_name: str,
    task_run_id: str,
) -> None:
    """
    Run the Bronze-to-Silver order_items incremental pipeline.
    """

    start_time = datetime.now(timezone.utc)

    try:

        # -------------------------------------------------
        # Transform Bronze batch
        # -------------------------------------------------

        silver_order_items_df = transform(
            spark=spark,
            run_date=run_date,
        )

        # -------------------------------------------------
        # Count records before merge
        # -------------------------------------------------

        before_count = (
            spark.table(SILVER_TABLE)
            .count()
        )

        logger.info(
            "Current Silver count before merge for table=%s: %s",
            TABLE_NAME,
            before_count,
        )

        # -------------------------------------------------
        # Count incoming batch
        # -------------------------------------------------

        incoming_count = silver_order_items_df.count()

        logger.info(
            "Incoming batch records for table=%s: %s",
            TABLE_NAME,
            incoming_count,
        )

        # -------------------------------------------------
        # Identify new records
        # -------------------------------------------------

        silver_order_items_existing_df = spark.table(
            SILVER_TABLE
        )

        incoming_inserts_df = (
            silver_order_items_df.alias("source")
            .join(
                silver_order_items_existing_df.alias("target"),
                F.col("source.order_item_id")
                == F.col("target.order_item_id"),
                "left_anti",
            )
        )

        insert_count = incoming_inserts_df.count()

        logger.info(
            "Insert summary for table=%s: incoming=%s, new_inserts=%s",
            TABLE_NAME,
            incoming_count,
            insert_count,
        )

        # -------------------------------------------------
        # Insert new order items
        # -------------------------------------------------

        logger.info(
            "Inserting new records into Silver table=%s",
            SILVER_TABLE,
        )

        insert_to_silver(
            spark=spark,
            df=silver_order_items_df,
            table_name=SILVER_TABLE,
            merge_key="target.order_item_id = source.order_item_id",
        )

        # -------------------------------------------------
        # Count records after merge
        # -------------------------------------------------

        after_count = (
            spark.table(SILVER_TABLE)
            .count()
        )

        logger.info(
            "Current Silver count after merge for table=%s: %s",
            TABLE_NAME,
            after_count,
        )

        end_time = datetime.now(timezone.utc)

        logger.info(
            "Successfully completed Silver pipeline for table=%s, run_date=%s. "
            "Before=%s, Incoming=%s, New inserts=%s, After=%s",
            TABLE_NAME,
            run_date,
            before_count,
            incoming_count,
            insert_count,
            after_count,
        )

        # -------------------------------------------------
        # Audit SUCCESS
        # -------------------------------------------------

        write_audit_log(
            spark=spark,
            pipeline_run_id=pipeline_run_id,
            pipeline_name=pipeline_name,
            job_name=job_name,
            job_run_id=job_run_id,
            task_name=task_name,
            task_run_id=task_run_id,
            layer="Silver",
            source_name=TABLE_NAME,
            load_type="INCREMENTAL",
            status="SUCCESS",
            row_count=insert_count,
            error_message=None,
            start_time=start_time,
            end_time=end_time,
        )

    except Exception as e:

        end_time = datetime.now(timezone.utc)

        logger.exception(
            "Silver pipeline failed for table=%s, run_date=%s",
            TABLE_NAME,
            run_date,
        )

        # -------------------------------------------------
        # Audit FAILURE
        # -------------------------------------------------

        try:

            write_audit_log(
                spark=spark,
                pipeline_run_id=pipeline_run_id,
                pipeline_name=pipeline_name,
                job_name=job_name,
                job_run_id=job_run_id,
                task_name=task_name,
                task_run_id=task_run_id,
                layer="Silver",
                source_name=TABLE_NAME,
                load_type="INCREMENTAL",
                status="FAILED",
                row_count=None,
                error_message=str(e)[:4000],
                start_time=start_time,
                end_time=end_time,
            )

        except Exception:
            logger.exception(
                "Failed to write audit record for table=%s",
                TABLE_NAME,
            )

        # Preserve the original pipeline failure.
        raise


# ---------------------------------------------------------
# Argument Parsing
# ---------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Parse Databricks Job parameters."""

    parser = argparse.ArgumentParser(
        description="Silver order_items pipeline",
    )

    parser.add_argument(
        "--run_date",
        required=True,
    )

    parser.add_argument(
        "--pipeline_name",
        required=True,
    )

    parser.add_argument(
        "--pipeline_run_id",
        required=True,
    )

    parser.add_argument(
        "--job_name",
        required=True,
    )

    parser.add_argument(
        "--job_run_id",
        required=True,
    )

    parser.add_argument(
        "--task_name",
        required=True,
    )

    parser.add_argument(
        "--task_run_id",
        required=True,
    )

    return parser.parse_args()


# ---------------------------------------------------------
# Entry Point
# ---------------------------------------------------------

def main() -> None:
    """Application entry point."""

    args = parse_args()

    spark = (
        SparkSession.builder
        .appName(args.task_name)
        .getOrCreate()
    )

    run(
        spark=spark,
        run_date=args.run_date,
        pipeline_run_id=args.pipeline_run_id,
        pipeline_name=args.pipeline_name,
        job_name=args.job_name,
        job_run_id=args.job_run_id,
        task_name=args.task_name,
        task_run_id=args.task_run_id,
    )


if __name__ == "__main__":
    main()