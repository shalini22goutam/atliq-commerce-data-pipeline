SELECT * from etl.audit_log
SELECT * from etl.control_table

SELECT * from etl.pipeline_run
SELECT * from etl.adf_audit_log
SELECT * from etl.databricks_audit_log

delete from  etl.pipeline_run 

delete from  etl.adf_audit_log;
delete from  etl.databricks_audit_log;
delete from  etl.pipeline_run;


@activity('Copy_SQL_Full_Load').ExecutionEndTime
@pipeline().RunId
@activity('Copy_FullLoad_Audit_Success').output.rowsCopied
@activity('Copy_SQL_Incremental_Load').output.rowsCopied
@activity('Copy_SQL_Incremental_Load').output.errors


ALTER TABLE dbo.customers
ADD test VARCHAR(20) NULL;