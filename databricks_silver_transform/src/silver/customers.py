"""Bronze-to-Silver pipeline for the customers table.

Reads Bronze customer records, applies cleaning/standardization
transforms, writes a full-refresh Silver table, and logs an audit
record for the run (success or failure).
"""

import argparse
from datetime import datetime, timezone
from pyspark.sql import DataFrame, SparkSession, functions as F

from databricks_jobs.src.audit.audit_logger import write_audit_log
from databricks_jobs.src.common.logger import get_logger
from databricks_jobs.src.common.silver_utils import get_silver_table, read_bronze_full, write_silver_full_refresh

logger = get_logger(__name__)

TABLE_NAME = "customers"
SILVER_TABLE = get_silver_table(TABLE_NAME)

def transform(spark: SparkSession) -> DataFrame:
    """
    Transform Bronze customers data into the Silver customers dataset.

    Transformations:
    - Trim and standardize city using initcap.
    - Convert signup_date to DATE.
    - Remove records with null customer_id.
    - Deduplicate customers by customer_id.
    """

    logger.info("Reading Bronze data for table=%s", TABLE_NAME)

    bronze_customers_df = read_bronze_full(spark=spark, adls_dir=TABLE_NAME)

    logger.info("Applying Silver transformations for table=%s", TABLE_NAME)

    return (
        bronze_customers_df.withColumn("city", F.initcap(F.trim(F.col("city"))))
                 .withColumn("signup_date", F.to_date(F.col("signup_date")))
                 .filter(F.col("customer_id").isNotNull())
                 .dropDuplicates(["customer_id"])
    )

def run(
    spark: SparkSession,
    pipeline_run_id: str,
    job_name: str,
    job_run_id: str,
    task_name: str,
    task_run_id: str,
) -> None:
    """
    Run the Bronze-to-Silver customers pipeline.

    Transforms and writes the Silver table, then writes a Success
    audit log with the resulting row count. On failure, writes a
    Fail audit log with the error message and re-raises.

    Args:
        spark: Active SparkSession.
        pipeline_run_id: Orchestration-level run identifier.
        job_name: Databricks job name.
        job_run_id: Databricks job run identifier.
        task_name: Databricks task name.
        task_run_id: Databricks task run identifier.

    Raises:
        Exception: Re-raises any error from transform/write after
            logging it and recording a Fail audit entry.
    """

    start_time = datetime.now(timezone.utc)

    try:
        silver_customers_df = transform(spark)

        logger.info("Writing Silver table=%s using full refresh", SILVER_TABLE)

        write_silver_full_refresh(spark=spark, df=silver_customers_df, table_name=SILVER_TABLE)

        # Count after successful write to avoid recomputing the Bronze-to-Silver transformation.
        row_count = spark.table(SILVER_TABLE).count()

        logger.info("Records written to Silver table=%s: %s", SILVER_TABLE, row_count)

        end_time = datetime.now(timezone.utc)

        logger.info("Successfully completed Silver pipeline for table=%s", TABLE_NAME)

        write_audit_log(
            spark=spark,
            run_date=None,
            pipeline_run_id=pipeline_run_id,
            job_name=job_name,
            job_run_id=job_run_id,
            task_name=task_name,
            task_run_id=task_run_id,
            layer="Silver",
            source_name=SILVER_TABLE,
            load_type="Full Load",
            status="Success",
            row_count=row_count,
            error_message=None,
            start_time=start_time,
            end_time=end_time
        )

    except Exception as e:

        end_time = datetime.now(timezone.utc)

        logger.exception("Silver pipeline failed for table=%s", TABLE_NAME)

        try:

            write_audit_log(
                spark=spark,
                run_date=None,
                pipeline_run_id=pipeline_run_id,
                job_name=job_name,
                job_run_id=job_run_id,
                task_name=task_name,
                task_run_id=task_run_id,
                layer="Silver",
                source_name=SILVER_TABLE,
                load_type="Full Load",
                status="Fail",
                row_count=None,
                error_message=str(e)[:4000],
                start_time=start_time,
                end_time=end_time
            )

        except Exception:
            logger.exception("Failed to write audit record for table=%s", TABLE_NAME,)

        # Preserve the original pipeline failure.
        raise


def parse_args() -> argparse.Namespace:
    """Parse Databricks Job parameters."""

    parser = argparse.ArgumentParser(description="Silver customers pipeline",)

    parser.add_argument("--pipeline_run_id",required=True)
    parser.add_argument("--job_name", required=True)
    parser.add_argument("--job_run_id", required=True)
    parser.add_argument("--task_name", required=True)
    parser.add_argument("--task_run_id", required=True)

    return parser.parse_args()

def main() -> None:
    """Parse CLI args, initialize Spark, and run the pipeline."""

    args = parse_args()

    spark = SparkSession.builder.appName(args.task_name).getOrCreate()
    
    run(
        spark=spark,
        pipeline_run_id=args.pipeline_run_id,
        job_name=args.job_name,
        job_run_id=args.job_run_id,
        task_name=args.task_name,
        task_run_id=args.task_run_id
    )


if __name__ == "__main__":
    main()