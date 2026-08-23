CREATE PROCEDURE etl.usp_log_adf_activity
    @pipeline_run_id VARCHAR(100),
    @layer           VARCHAR(50),
    @source_name     VARCHAR(200),
    @activity_name   VARCHAR(200),
    @load_type       VARCHAR(20),
    @status          VARCHAR(20),
    @row_count       BIGINT = NULL,
    @error_message   VARCHAR(MAX) = NULL,
    @start_time      DATETIME2,
    @end_time        DATETIME2
AS
BEGIN
    INSERT INTO etl.adf_audit_log
        (pipeline_run_id, layer, source_name, activity_name, load_type,
         status, row_count, error_message, start_time, end_time)
    VALUES
        (@pipeline_run_id, @layer, @source_name, @activity_name, @load_type,
         @status, @row_count, @error_message, @start_time, @end_time);
END
GO