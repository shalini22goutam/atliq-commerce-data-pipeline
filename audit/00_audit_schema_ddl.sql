IF SCHEMA_ID('etl') IS NULL
    EXEC('CREATE SCHEMA etl');
GO

DROP TABLE IF EXISTS etl.databricks_audit_log;
DROP TABLE IF EXISTS etl.adf_audit_log;
DROP TABLE IF EXISTS etl.pipeline_run;
GO

-- One row per ADF pipeline run
CREATE TABLE etl.pipeline_run
(
    pipeline_run_id   VARCHAR(100) PRIMARY KEY,
    run_date          VARCHAR(20)  NOT NULL,
    pipeline_name     VARCHAR(200),
    created_at        DATETIME2 DEFAULT GETDATE()
);
GO

-- One row per ADF activity, many can belong to one pipeline_run
CREATE TABLE etl.adf_audit_log
(
    adf_audit_id      BIGINT IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id   VARCHAR(100) NOT NULL,
    layer             VARCHAR(50),
    source_name       VARCHAR(200),
    activity_name     VARCHAR(200),
    load_type         VARCHAR(20),
    status            VARCHAR(20),
    row_count         BIGINT,
    error_message     VARCHAR(MAX),
    start_time        DATETIME2,
    end_time          DATETIME2,
    created_at        DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT fk_adf_audit_pipeline_run FOREIGN KEY (pipeline_run_id)
        REFERENCES etl.pipeline_run (pipeline_run_id)
);
GO

-- One row per Databricks task, many can belong to one pipeline_run
CREATE TABLE etl.databricks_audit_log
(
    databricks_audit_id  BIGINT IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id       VARCHAR(100) NOT NULL,
    run_date              VARCHAR(20)
    job_name              VARCHAR(200),
    job_run_id            VARCHAR(100),
    task_name             VARCHAR(100),
    task_run_id            VARCHAR(100),
    layer                 VARCHAR(50),
    source_name           VARCHAR(200),
    load_type             VARCHAR(20),
    status                VARCHAR(20),
    row_count             BIGINT,
    error_message         VARCHAR(MAX),
    start_time            DATETIME2,
    end_time              DATETIME2,
    created_at            DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT fk_databricks_audit_pipeline_run FOREIGN KEY (pipeline_run_id)
        REFERENCES etl.pipeline_run (pipeline_run_id)
);
GO

CREATE NONCLUSTERED INDEX ix_adf_audit_pipeline_run_id
    ON etl.adf_audit_log (pipeline_run_id);
GO

CREATE NONCLUSTERED INDEX ix_databricks_audit_pipeline_run_id
    ON etl.databricks_audit_log (pipeline_run_id);
GO