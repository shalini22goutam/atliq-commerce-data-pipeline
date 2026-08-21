from pyspark.sql import DataFrame, functions as F

from databricks_notebooks.src.common.logger import get_logger
from databricks_notebooks.src.common.silver_utils import (
    get_silver_table,
    read_bronze_full,
    write_silver_full_refresh,
)


# Source table name in Bronze.
TABLE_NAME = "supplier_price_list"


# Create logger for this Silver transformation.
logger = get_logger("silver_supplier_price_list")


def transform(spark) -> DataFrame:
    """
    Transform Bronze supplier price list data into the Silver dataset.

    Transformations:
    - Convert effective_date to DATE.
    - Cast supplier_cost to DECIMAL(12,2).
    - Trim product_name.
    - Trim supplier_name.
    - Remove records with null product_id.
    - Remove records with null supplier_cost.
    """

    logger.info(
        "Reading Bronze data for table=%s",
        TABLE_NAME,
    )

    bronze_supplier_price_list_df = read_bronze_full(
        spark=spark,
        table_name=TABLE_NAME,
    )

    logger.info(
        "Applying Silver transformations for table=%s",
        TABLE_NAME,
    )

    return (
        bronze_supplier_price_list_df
        .withColumn(
            "effective_date",
            F.to_date(F.col("effective_date"))
        )
        .withColumn(
            "supplier_cost",
            F.col("supplier_cost").cast("decimal(12,2)")
        )
        .withColumn(
            "product_name",
            F.trim(F.col("product_name"))
        )
        .withColumn(
            "supplier_name",
            F.trim(F.col("supplier_name"))
        )
        .filter(
            F.col("product_id").isNotNull()
        )
        .filter(
            F.col("supplier_cost").isNotNull()
        )
    )


# Databricks provides the SparkSession as `spark`.

logger.info(
    "Starting Silver pipeline for table=%s",
    TABLE_NAME,
)

silver_supplier_price_list_df = transform(spark)

silver_table = get_silver_table(TABLE_NAME)

# Count records that will be written to Silver.
records_written = silver_supplier_price_list_df.count()

logger.info(
    "Writing Silver table=%s using full refresh. Records=%s",
    silver_table,
    records_written,
)

write_silver_full_refresh(
    df=silver_supplier_price_list_df,
    table_name=silver_table,
)

logger.info(
    "Successfully completed Silver pipeline for table=%s. Records written=%s",
    TABLE_NAME,
    records_written,
)