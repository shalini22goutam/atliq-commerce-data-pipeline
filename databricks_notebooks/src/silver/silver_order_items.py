from pyspark.sql import DataFrame, functions as F

from databricks_notebooks.src.common.logger import get_logger
from databricks_notebooks.src.common.silver_utils import (
    get_silver_table,
    read_bronze_batch,
    insert_to_silver,
)


# Source table name in Bronze.
TABLE_NAME = "order_items"


# Create logger for this Silver transformation.
logger = get_logger("silver_order_items")


def transform(spark, run_date: str) -> DataFrame:
    """
    Transform the Bronze order_items batch into the Silver order_items dataset.

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


# Databricks provides the SparkSession as `spark`.
# The job provides run_date through the Databricks widget.

#run_date = dbutils.widgets.get("run_date")
run_date ='2026-08-18'

logger.info(
    "Starting Silver pipeline for table=%s, run_date=%s",
    TABLE_NAME,
    run_date,
)

silver_order_items_df = transform(
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


# Count the records in the incoming batch.
incoming_count = silver_order_items_df.count()

logger.info(
    "Incoming batch records for table=%s: %s",
    TABLE_NAME,
    incoming_count,
)


# Identify incoming order items that do not already exist in Silver.
# Since order items are immutable, existing records are not updated.
silver_order_items_existing_df = spark.table(silver_table)

incoming_inserts_df = (
    silver_order_items_df.alias("source")
    .join(
        silver_order_items_existing_df.alias("target"),
        F.col("source.order_item_id")
        == F.col("target.order_item_id"),
        "left_anti",
    )
)


# Count records that will actually be inserted.
insert_count = incoming_inserts_df.count()

logger.info(
    "Insert summary for table=%s: incoming=%s, new_inserts=%s",
    TABLE_NAME,
    incoming_count,
    insert_count,
)


# Insert only new order items into Silver.
insert_to_silver(
    spark=spark,
    df=silver_order_items_df,
    table_name=silver_table,
    merge_key="target.order_item_id = source.order_item_id",
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
    "Before=%s, Incoming=%s, New inserts=%s, After=%s",
    TABLE_NAME,
    run_date,
    before_count,
    incoming_count,
    insert_count,
    after_count,
)