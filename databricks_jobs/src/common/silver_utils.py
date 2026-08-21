from pyspark.sql import DataFrame, SparkSession

from databricks_jobs.src.common.constants import (
    BRONZE_SCHEMA,
    CATALOG_NAME,
    SILVER_SCHEMA,
)


# ---------------------------------------------------------
# Table Utilities
# ---------------------------------------------------------

def get_silver_table(
    table_name: str,
) -> str:
    """
    Return the fully qualified Silver table name.
    """

    return f"{CATALOG_NAME}.{SILVER_SCHEMA}.{table_name}"


# ---------------------------------------------------------
# Bronze Read Utilities
# ---------------------------------------------------------

def read_bronze_full(
    spark: SparkSession,
    table_name: str,
) -> DataFrame:
    """
    Read the complete Bronze table.
    """

    bronze_table = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.{table_name}"

    return spark.table(bronze_table)


def read_bronze_batch(
    spark: SparkSession,
    table_name: str,
    run_date: str,
) -> DataFrame:
    """
    Read a specific ingestion-date partition from Bronze.
    """

    bronze_table = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.{table_name}"

    return (
        spark.table(bronze_table)
        .filter(
            f"ingestion_date = '{run_date}'"
        )
    )


# ---------------------------------------------------------
# Data Transformation Utilities
# ---------------------------------------------------------

def dedupe_latest(
    df: DataFrame,
    key_col: str,
    order_col: str,
) -> DataFrame:
    """
    Keep the latest record for each key based on the
    specified ordering column.
    """

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    window_spec = (
        Window
        .partitionBy(key_col)
        .orderBy(F.col(order_col).desc())
    )

    return (
        df
        .withColumn(
            "_row_number",
            F.row_number().over(window_spec),
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop("_row_number")
    )


# ---------------------------------------------------------
# Silver Write Utilities
# ---------------------------------------------------------

def write_silver_full_refresh(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
) -> None:
    """
    Write a DataFrame to a Silver table using full refresh.
    """

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )


def insert_to_silver(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    merge_key: str,
) -> None:
    """
    Insert records into Silver when they do not already exist.
    """

    from delta.tables import DeltaTable

    target = DeltaTable.forName(
        spark,
        table_name,
    )

    (
        target.alias("target")
        .merge(
            df.alias("source"),
            merge_key,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def upsert_to_silver(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    merge_key: str,
    update_condition: str,
) -> None:
    """
    Upsert records into Silver.

    Existing records are updated only when the supplied
    update condition is satisfied. New records are inserted.
    """

    from delta.tables import DeltaTable

    target = DeltaTable.forName(
        spark,
        table_name,
    )

    (
        target.alias("target")
        .merge(
            df.alias("source"),
            merge_key,
        )
        .whenMatchedUpdateAll(
            condition=update_condition,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )