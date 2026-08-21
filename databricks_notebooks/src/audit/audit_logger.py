from datetime import datetime, timezone


# ---------------------------------------------------------
# Azure SQL connection details
# ---------------------------------------------------------

server = "atliq-sql-server.database.windows.net"
database = "atliq_commerce"
username = "atliq_admin"
password = "Krishna@1956"


jdbc_url = (
    f"jdbc:sqlserver://{server}:1433;"
    f"database={database};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30;"
)

gateway = spark.sparkContext._gateway

conn = gateway.jvm.java.sql.DriverManager.getConnection(
    jdbc_url,
    username,
    password
)

try:
    sql = """
        INSERT INTO etl.audit_log
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
            end_time
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    stmt = conn.prepareStatement(sql)

    stmt.setString(1, "TEST-001")
    stmt.setString(2, "pl_staynest")
    stmt.setString(3, "Databricks")
    stmt.setString(4, "StayNest Databricks Job")
    stmt.setString(5, "TEST-JOB-001")
    stmt.setString(6, "TEST-TASK-001")
    stmt.setString(7, "Silver")
    stmt.setString(8, "customers")
    stmt.setString(9, "Silver Notebook")
    stmt.setString(10, "Incremental")
    stmt.setString(11, "SUCCESS")

    stmt.setLong(12, 1000)

    stmt.setNull(
        13,
        gateway.jvm.java.sql.Types.VARCHAR
    )

    now = datetime.now(timezone.utc)

    timestamp = gateway.jvm.java.sql.Timestamp(
        int(now.timestamp() * 1000)
    )

    stmt.setTimestamp(14, timestamp)
    stmt.setTimestamp(15, timestamp)

    stmt.executeUpdate()

    print("Audit record inserted successfully.")

finally:
    stmt.close()
    conn.close()