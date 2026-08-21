from pyspark.sql import DataFrame, functions as F

from databricks_notebooks.src.common.logger import get_logger
from databricks_notebooks.src.common.silver_utils import (
    get_silver_table,
    read_bronze_full,
    write_silver_full_refresh,
)


# Source table name in Bronze.
TABLE_NAME = "products"


# Create logger for this Silver transformation.
logger = get_logger("silver_products")


def transform(spark) -> DataFrame:
    """
    Transform Bronze products data into the Silver products dataset.

    Transformations:
    - Trim product_name.
    - Trim category.
    - Remove records with null product_id.
    - Deduplicate products by product_id.
    """

    logger.info(
        "Reading Bronze data for table=%s",
        TABLE_NAME,
    )

    bronze_products_df = read_bronze_full(
        spark=spark,
        table_name=TABLE_NAME,
    )

    logger.info(
        "Applying Silver transformations for table=%s",
        TABLE_NAME,
    )

    return (
        bronze_products_df
        .withColumn(
            "product_name",
            F.trim(F.col("product_name"))
        )
        .withColumn(
            "category",
            F.trim(F.col("category"))
        )
        .filter(
            F.col("product_id").isNotNull()
        )
        .dropDuplicates(["product_id"])
    )


# Databricks provides the SparkSession as `spark`.

logger.info(
    "Starting Silver pipeline for table=%s",
    TABLE_NAME,
)

silver_products_df = transform(spark)

silver_table = get_silver_table(TABLE_NAME)

# Count records that will be written to Silver.
records_written = silver_products_df.count()

logger.info(
    "Writing Silver table=%s using full refresh",
    silver_table
)

write_silver_full_refresh(
    df=silver_products_df,
    table_name=silver_table,
)

logger.info(
    "Successfully completed Silver pipeline for table=%s. Records written: %s",
    TABLE_NAME,
    records_written,
)