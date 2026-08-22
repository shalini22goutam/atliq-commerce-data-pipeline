IF SCHEMA_ID('etl') IS NULL
    EXEC('CREATE SCHEMA etl');

GO

DROP TABLE IF EXISTS etl.audit_log;

GO

CREATE TABLE etl.audit_log
(
    audit_id          BIGINT IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id   VARCHAR(100),
    pipeline_name     VARCHAR(200),
    orchestrator      VARCHAR(50),
    job_name          VARCHAR(200),
    job_run_id        VARCHAR(100),
    task_run_id       VARCHAR(100),
    layer             VARCHAR(50),
    source_name       VARCHAR(200),
    activity_name     VARCHAR(200),
    load_type         VARCHAR(20),
    status            VARCHAR(20),
    row_count         BIGINT,
    error_message     VARCHAR(MAX),
    start_time        DATETIME2,
    end_time          DATETIME2,
    created_at        DATETIME2 DEFAULT GETDATE()
);
GO



SELECT * from etl.audit_log