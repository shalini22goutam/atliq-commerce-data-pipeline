from pyspark.sql import DataFrame, SparkSession, functions as F, Window
from delta.tables import DeltaTable

from databricks_notebooks.src.common.constants import (
    BRONZE_PATH,
    CATALOG_NAME,
    SILVER_SCHEMA,
)

def read_bronze_full(
    spark: SparkSession,
    table_name: str,
) -> DataFrame:
    """
    Read the complete Bronze dataset for a given table.

    Used for tables that follow a full-refresh processing strategy.
    """

    return spark.read.parquet(
        f"{BRONZE_PATH}/{table_name}"
    )


def read_bronze_batch(
    spark: SparkSession,
    table_name: str,
    run_date: str,
) -> DataFrame:
    """
    Read a specific ingestion-date partition from Bronze.

    Used for incremental/batch processing.
    """

    return spark.read.parquet(
        f"{BRONZE_PATH}/{table_name}/ingest_date={run_date}"
    )


def dedupe_latest(
    df: DataFrame,
    key_col: str,
    order_col: str = "updated_at",
) -> DataFrame:
    """
    Keep the latest record for each business key.

    The latest record is determined using the specified ordering column,
    which defaults to updated_at.
    """

    w = (
        Window
        .partitionBy(key_col)
        .orderBy(F.col(order_col).desc())
    )

    return (
        df
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def get_silver_table(
    table_name: str,
) -> str:
    """
    Return the fully qualified Silver table name.
    """

    return f"{CATALOG_NAME}.{SILVER_SCHEMA}.{table_name}"


def write_silver_full_refresh(
    df: DataFrame,
    table_name: str,
) -> None:
    """
    Write a Silver table using full-refresh semantics.

    The existing Silver table is overwritten with the
    transformed DataFrame.
    """

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )


def upsert_to_silver(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    merge_key: str,
    update_condition: str,
) -> None:
    """
    Upsert records into a Silver Delta table.

    Existing records are updated when the update condition
    is satisfied, while new records are inserted.
    """

    # Create the Silver table if it does not already exist.
    (
        DeltaTable
        .createIfNotExists(spark)
        .tableName(table_name)
        .addColumns(df.schema)
        .execute()
    )

    # Merge incoming records into the Silver Delta table.
    (
        DeltaTable
        .forName(spark, table_name)
        .alias("target")
        .merge(
            df.alias("source"),
            merge_key,
        )
        .whenMatchedUpdateAll(
            condition=update_condition
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def insert_to_silver(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    merge_key: str,
) -> None:
    """
    Insert only new records into a Silver Delta table.

    Existing records matching the merge key are left unchanged.
    """

    # Create the Silver table if it does not already exist.
    (
        DeltaTable
        .createIfNotExists(spark)
        .tableName(table_name)
        .addColumns(df.schema)
        .execute()
    )

    # Insert only records that do not already exist in Silver.
    (
        DeltaTable
        .forName(spark, table_name)
        .alias("target")
        .merge(
            df.alias("source"),
            merge_key,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )