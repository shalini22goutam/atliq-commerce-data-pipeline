CREATE OR ALTER PROCEDURE etl.usp_audit_log
    @pipeline_run_id VARCHAR(100),
    @pipeline_name   VARCHAR(200),
    @orchestrator    VARCHAR(50),
    @job_name        VARCHAR(200) = NULL,
    @job_run_id      VARCHAR(100) = NULL,
    @task_run_id     VARCHAR(100) = NULL,
    @layer           VARCHAR(50),
    @source_name     VARCHAR(200) = NULL,
    @activity_name   VARCHAR(200),
    @load_type       VARCHAR(20) = NULL,
    @status          VARCHAR(20),
    @row_count       BIGINT = NULL,
    @error_message   VARCHAR(MAX) = NULL,
    @start_time      DATETIME2,
    @end_time        DATETIME2
AS
BEGIN
    SET NOCOUNT ON;

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
    VALUES
    (
        @pipeline_run_id,
        @pipeline_name,
        @orchestrator,
        @job_name,
        @job_run_id,
        @task_run_id,
        @layer,
        @source_name,
        @activity_name,
        @load_type,
        @status,
        @row_count,
        @error_message,
        @start_time,
        @end_time
    );
END;
GO




