from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from delta.tables import DeltaTable
from databricks_jobs.src.common.constants import (
    BRONZE_PATH,
    CATALOG_NAME,
    SILVER_SCHEMA,
)


def get_silver_table(table_name: str) -> str:
    """
    Return the fully qualified Silver table name.
    """

    return f"{CATALOG_NAME}.{SILVER_SCHEMA}.{table_name}"

def read_bronze_full(spark: SparkSession, adls_dir: str) -> DataFrame:
    """
    Read the complete Bronze table.
    """

    bronze_dir = f"{BRONZE_PATH}/{adls_dir}"

    return spark.read.parquet(bronze_dir)


def read_bronze_batch(spark: SparkSession, adls_dir: str, run_date: str) -> DataFrame:
    """
    Read a specific date based directory from Bronze.
    """

    return (
        spark.read.parquet(f"{BRONZE_PATH}/{adls_dir}/{run_date}")
    )

def dedupe_latest(df: DataFrame, key_col: str, order_col: str) -> DataFrame:
    """
    Keep the latest record for each key based on the
    specified ordering column.
    """

    window_spec = (
        Window
        .partitionBy(key_col)
        .orderBy(F.col(order_col).desc())
    )

    return (
        df
        .withColumn("_row_number", F.row_number().over(window_spec))
        .filter( F.col("_row_number") == 1)
        .drop("_row_number")
    )

def write_silver_full_refresh(spark: SparkSession, df: DataFrame,table_name: str) -> None:
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