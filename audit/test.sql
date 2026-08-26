
SELECT * from etl.control_table;
SELECT * from etl.pipeline_run;
SELECT * from etl.adf_audit_log
SELECT * from etl.databricks_audit_log;
select  task_run_id,  task_name, layer,source_name, load_type, status, row_count, run_date, error_message, start_time, end_time
from etl.databricks_audit_log

delete from  etl.pipeline_run 


ALTER TABLE dbo.customers
ADD test VARCHAR(20) NULL;

delete from  dbo.payments
delete from dbo.order_items
delete from dbo.orders
delete from dbo.products
delete from dbo.customers


truncate Table etl.control_table;
delete from  etl.adf_audit_log;
delete from  etl.databricks_audit_log;
delete from  etl.pipeline_run;

SELECT 'customers' AS tbl,COUNT(*) AS [rows] FROM dbo.customers
UNION ALL
SELECT 'products',COUNT(*) FROM dbo.products
UNION ALL
SELECT 'orders',COUNT(*) FROM dbo.orders
UNION ALL
SELECT 'order_items',COUNT(*) FROM dbo.order_items
UNION ALL
SELECT 'payments',COUNT(*) FROM dbo.payments;


select  task_run_id,  task_name, layer,source_name, load_type, status, row_count, run_date, error_message, start_time, end_time
from etl.databricks_audit_log

select * from dbo.orders where created_at > '2026-08-23'

SELECT * FROM sys.tables 
WHERE name = 'etl.control_table' 
AND temporal_type = 2