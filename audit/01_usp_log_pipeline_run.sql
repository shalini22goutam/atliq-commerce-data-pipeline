CREATE PROCEDURE etl.usp_log_pipeline_run
    @pipeline_run_id VARCHAR(100),
    @run_date        VARCHAR(20),
    @pipeline_name   VARCHAR(200)
AS
BEGIN
    IF NOT EXISTS (SELECT 1 FROM etl.pipeline_run WHERE pipeline_run_id = @pipeline_run_id)
        INSERT INTO etl.pipeline_run (pipeline_run_id, run_date, pipeline_name)
        VALUES (@pipeline_run_id, @run_date, @pipeline_name);
END
GO