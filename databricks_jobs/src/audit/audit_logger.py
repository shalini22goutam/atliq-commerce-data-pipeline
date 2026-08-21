from datetime import datetime, timezone


# ---------------------------------------------------------
# SQL connection configuration
# ---------------------------------------------------------

SERVER = "atliq-sql-server.database.windows.net"
DATABASE = "atliq_commerce"

USERNAME = dbutils.secrets.get(
    scope="azure-sql-scope",
    key="sql-db-username",
)

PASSWORD = dbutils.secrets.get(
    scope="azure-sql-scope",
    key="sql-db-password",
)

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
    jdbc_url: str,
    username: str,
    password: str,
    pipeline_run_id: str,
    pipeline_name: str,
    orchestrator: str,
    job_name: str,
    job_run_id: str,
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
        """
        pipeline_run_id string,
        pipeline_name string,
        orchestrator string,
        job_name string,
        job_run_id string,
        task_run_id string,
        layer string,
        source_name string,
        activity_name string,
        load_type string,
        status string,
        row_count long,
        error_message string,
        start_time timestamp,
        end_time timestamp
        """,
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


# ---------------------------------------------------------
# Test audit insert
# ---------------------------------------------------------

now = datetime.now(timezone.utc)

write_audit_log(
    jdbc_url=JDBC_URL,
    username=USERNAME,
    password=PASSWORD,
    pipeline_run_id="TEST-001",
    pipeline_name="atliq_commerce",
    orchestrator="Databricks",
    job_name="Audit Test",
    job_run_id="TEST-JOB-001",
    task_run_id="TEST-TASK-001",
    layer="Silver",
    source_name="customers",
    activity_name="silver_customers",
    load_type="FULL_REFRESH",
    status="SUCCESS",
    row_count=1000,
    error_message=None,
    start_time=now,
    end_time=now,
)

print("Audit record inserted successfully.")