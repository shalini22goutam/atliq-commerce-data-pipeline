from pyspark.sql import DataFrame, functions as F

from databricks_notebooks.src.common.logger import get_logger
from databricks_notebooks.src.common.silver_utils import (
    get_silver_table,
    read_bronze_full,
    write_silver_full_refresh,
)


# Source table name in Bronze.
TABLE_NAME = "customers"


# Create logger for this Silver transformation.
logger = get_logger("silver_customers")


def transform(spark) -> DataFrame:
    """
    Transform Bronze customers data into the Silver customers dataset.

    Transformations:
    - Trim and standardize city names using initcap.
    - Convert signup_date to DATE.
    - Remove duplicate customer records.
    - Remove records with a null customer_id.
    """

    logger.info(
        "Reading Bronze data for table=%s",
        TABLE_NAME,
    )

    bronze_customers_df = read_bronze_full(
        spark=spark,
        table_name=TABLE_NAME,
    )

    logger.info(
        "Applying Silver transformations for table=%s",
        TABLE_NAME,
    )

    return (
        bronze_customers_df
        .withColumn(
            "city",
            F.initcap(F.trim("city"))
        )
        .withColumn(
            "signup_date",
            F.to_date("signup_date")
        )
        .dropDuplicates(["customer_id"])
        .filter(
            F.col("customer_id").isNotNull()
        )
    )


# Databricks provides the SparkSession as `spark`.

logger.info(
    "Starting Silver pipeline for table=%s",
    TABLE_NAME,
)

silver_customers_df = transform(spark)

silver_table = get_silver_table(TABLE_NAME)

# Count records that will be written to Silver.
records_written = silver_customers_df.count()

logger.info(
    "Writing Silver table=%s using full refresh",
    silver_table,
    
)

write_silver_full_refresh(
    df=silver_customers_df,
    table_name=silver_table,
)

logger.info(
    "Successfully completed Silver pipeline for table=%s. Records written:%s",
    TABLE_NAME,
    records_written,
)