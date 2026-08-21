from pyspark.sql import DataFrame, functions as F

from databricks_notebooks.src.common.logger import get_logger
from databricks_notebooks.src.common.silver_utils import (
    get_silver_table,
    read_bronze_batch,
    dedupe_latest,
    upsert_to_silver,
)


# Source table name in Bronze.
TABLE_NAME = "orders"


# Create logger for this Silver transformation.
logger = get_logger("silver_orders")


def transform(spark, run_date: str) -> DataFrame:
    """
    Transform the Bronze orders batch into the Silver orders dataset.

    Transformations:
    - Read only the specified ingestion-date partition.
    - Keep the latest version of each order within the batch.
    - Convert order_date to DATE.
    - Cast order_amount to DECIMAL(12,2).
    - Remove records with null order_id.
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
        "Deduplicating latest records for table=%s",
        TABLE_NAME,
    )

    orders_df = dedupe_latest(
        df=batch_df,
        key_col="order_id",
        order_col="updated_at",
    )

    logger.info(
        "Applying Silver transformations for table=%s",
        TABLE_NAME,
    )

    return (
        orders_df
        .withColumn(
            "order_date",
            F.to_date("order_date")
        )
        .withColumn(
            "order_amount",
            F.col("order_amount").cast("decimal(12,2)")
        )
        .filter(
            F.col("order_id").isNotNull()
        )
    )


# Databricks provides the SparkSession as `spark`.
# The job provides run_date through the Databricks widget.

#run_date = dbutils.widgets.get("run_date")
run_date ='2026-08-18'

logger.info(
    "Starting Silver pipeline for table=%s, run_date=%s",
    TABLE_NAME,
    run_date,
)

silver_orders_df = transform(
    spark=spark,
    run_date=run_date,
)

silver_table = get_silver_table(TABLE_NAME)


# Get the current Silver record count before the merge.
before_count = (
    spark.table(silver_table)
    .count()
)

logger.info(
    "Current Silver count before merge for table=%s: %s",
    TABLE_NAME,
    before_count,
)


# Count the incoming records in the current batch.
incoming_count = silver_orders_df.count()

logger.info(
    "Incoming batch records for table=%s: %s",
    TABLE_NAME,
    incoming_count,
)


# Read the existing Silver data for determining updates and inserts.
silver_orders_existing_df = spark.table(silver_table)


# Identify incoming records that already exist in Silver
# and have a newer updated_at timestamp.
incoming_updates_df = (
    silver_orders_df.alias("source")
    .join(
        silver_orders_existing_df.alias("target"),
        F.col("source.order_id") == F.col("target.order_id"),
        "inner",
    )
    .filter(
        F.col("source.updated_at") > F.col("target.updated_at")
    )
)


# Count records that will actually be updated.
update_count = incoming_updates_df.count()


# Identify incoming records that do not already exist in Silver.
incoming_inserts_df = (
    silver_orders_df.alias("source")
    .join(
        silver_orders_existing_df.alias("target"),
        F.col("source.order_id") == F.col("target.order_id"),
        "left_anti",
    )
)


# Count records that will be inserted.
insert_count = incoming_inserts_df.count()


logger.info(
    "Upsert summary for table=%s: incoming=%s, updates=%s, inserts=%s",
    TABLE_NAME,
    incoming_count,
    update_count,
    insert_count,
)


# Perform the actual upsert into Silver.
upsert_to_silver(
    spark=spark,
    df=silver_orders_df,
    table_name=silver_table,
    merge_key="target.order_id = source.order_id",
    update_condition="source.updated_at > target.updated_at",
)


# Get the Silver record count after the merge.
after_count = (
    spark.table(silver_table)
    .count()
)

logger.info(
    "Current Silver count after merge for table=%s: %s",
    TABLE_NAME,
    after_count,
)

logger.info(
    "Successfully completed Silver pipeline for table=%s, run_date=%s. "
    "Before=%s, Incoming=%s, Updates=%s, Inserts=%s, After=%s",
    TABLE_NAME,
    run_date,
    before_count,
    incoming_count,
    update_count,
    insert_count,
    after_count,
)