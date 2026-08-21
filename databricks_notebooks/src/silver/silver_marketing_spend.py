from pyspark.sql import DataFrame, functions as F

from databricks_notebooks.src.common.logger import get_logger
from databricks_notebooks.src.common.silver_utils import (
    get_silver_table,
    read_bronze_full,
    write_silver_full_refresh,
)


# Source table name in Bronze.
TABLE_NAME = "marketing_spend"


# Create logger for this Silver transformation.
logger = get_logger("silver_marketing_spend")


def transform(spark) -> DataFrame:
    """
    Transform Bronze marketing spend data into the Silver dataset.

    Transformations:
    - Convert spend_date to DATE.
    - Cast spend_amount to DECIMAL(12,2).
    - Trim channel.
    - Trim campaign.
    - Remove records with null spend_date.
    - Remove records with null spend_amount.
    - Remove records with null clicks.
    """

    logger.info(
        "Reading Bronze data for table=%s",
        TABLE_NAME,
    )

    bronze_marketing_spend_df = read_bronze_full(
        spark=spark,
        table_name=TABLE_NAME,
    )

    logger.info(
        "Applying Silver transformations for table=%s",
        TABLE_NAME,
    )

    return (
        bronze_marketing_spend_df
        .withColumn(
            "spend_date",
            F.to_date(F.col("spend_date"))
        )
        .withColumn(
            "spend_amount",
            F.col("spend_amount").cast("decimal(12,2)")
        )
        .withColumn(
            "channel",
            F.trim(F.col("channel"))
        )
        .withColumn(
            "campaign",
            F.trim(F.col("campaign"))
        )
        .filter(
            F.col("spend_date").isNotNull()
        )
        .filter(
            F.col("spend_amount").isNotNull()
        )
        .filter(
            F.col("clicks").isNotNull()
        )
    )


# Databricks provides the SparkSession as `spark`.

logger.info(
    "Starting Silver pipeline for table=%s",
    TABLE_NAME,
)

silver_marketing_spend_df = transform(spark)

silver_table = get_silver_table(TABLE_NAME)

# Count records that will be written to Silver.
records_written = silver_marketing_spend_df.count()

logger.info(
    "Writing Silver table=%s using full refresh. Records=%s",
    silver_table,
    records_written,
)

write_silver_full_refresh(
    df=silver_marketing_spend_df,
    table_name=silver_table,
)

logger.info(
    "Successfully completed Silver pipeline for table=%s. Records written=%s",
    TABLE_NAME,
    records_written,
)