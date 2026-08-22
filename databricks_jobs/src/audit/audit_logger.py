from datetime import datetime, timezone
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)

# SQL connection configuration

SERVER = "atliq-sql-server.database.windows.net"
DATABASE = "atliq_commerce"

JDBC_URL = (
    f"jdbc:sqlserver://{SERVER}:1433;"
    f"database={DATABASE};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30;"
)


AUDIT_SCHEMA = StructType([
    StructField("run_date", StringType(), True),
    StructField("pipeline_run_id", StringType(), True),
    StructField("pipeline_name", StringType(), True),
    StructField("orchestrator", StringType(), True),
    StructField("job_name", StringType(), True),
    StructField("job_run_id", StringType(), True),
    StructField("task_name", StringType(), True),
    StructField("task_run_id", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("source_name", StringType(), True),
    StructField("activity_name", StringType(), True),
    StructField("load_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("row_count", IntegerType(), True),
    StructField("error_message", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
])

# ---------------------------------------------------------
# Audit log writer
# ---------------------------------------------------------

def write_audit_log(
    spark: SparkSession,
    run_date: str,
    pipeline_run_id: str,
    pipeline_name: str,
    orchestrator: str,
    job_name: str,
    job_run_id: str,
    task_name: str,
    task_run_id: str,
    layer: str,
    source_name: str,
    activity_name: str,
    load_type: str,
    status: str,
    row_count: int | None = None,
    error_message: str | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> None:
    """Write one pipeline execution record to the Azure SQL audit table."""

    dbutils = DBUtils(spark)

    USERNAME = dbutils.secrets.get(scope="azure-sql-scope", key="sql-db-username")
    PASSWORD = dbutils.secrets.get(scope="azure-sql-scope", key="sql-db-password")

    start_time = start_time or datetime.now(timezone.utc)
    end_time = end_time or datetime.now(timezone.utc)

    audit_df = spark.createDataFrame(
        [
            (
                pipeline_run_id,
                run_date,
                pipeline_name,
                orchestrator,
                job_name,
                job_run_id,
                task_name,
                task_run_id,
                layer,
                source_name,
                activity_name,
                load_type,
                status,
                row_count,
                error_message,
                start_time,
                end_time,
            )
        ],
         schema=AUDIT_SCHEMA,
    )

    (
        audit_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "etl.audit_log")
        .option("user", USERNAME)
        .option("password", PASSWORD)
        .mode("append")
        .save()
    )

