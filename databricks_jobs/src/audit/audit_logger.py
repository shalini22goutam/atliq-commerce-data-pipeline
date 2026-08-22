from datetime import datetime, timezone
from pyspark.sql import DataFrame, SparkSession, functions as F

# ---------------------------------------------------------
# SQL connection configuration
# ---------------------------------------------------------

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


# ---------------------------------------------------------
# Audit log writer
# ---------------------------------------------------------

def write_audit_log(
    spark: SparkSession,
    jdbc_url: str,
    username: str,
    password: str,
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

    start_time = start_time or datetime.now(timezone.utc)
    end_time = end_time or datetime.now(timezone.utc)

    audit_df = spark.createDataFrame(
        [
            (
                pipeline_run_id,
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
    )

    (
        audit_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "etl.audit_log")
        .option("user", username)
        .option("password", password)
        .mode("append")
        .save()
    )

