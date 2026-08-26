from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

SQL_WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/9011d211f75286f8"   

default_args = {
    "owner": "atliq-data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="atliq_streaming_ops",
    start_date=datetime(2026, 8, 25),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["atliq", "phase2", "streaming"],
) as dag:

    check_fresh_events  = DatabricksSqlOperator(
                                task_id="fresh_events_2hours",
                                databricks_conn_id="databricks_default",
                                sql="""
                                    SELECT assert_true(
                                        COUNT(*) > 0,
                                        'DQ FAILED: No events landed in silver_order_events in the last 2 hours'
                                    )
                                    FROM atliq.streaming.silver_order_events
                                    WHERE event_ts >= current_timestamp() - INTERVAL 2 HOURS
                                """,
                                http_path=SQL_WAREHOUSE_HTTP_PATH,
                            )
    
    optimize_tables = DatabricksSqlOperator(
                                task_id="optimize_delta_tables",
                                databricks_conn_id="databricks_default",
                                sql="""
                                    OPTIMIZE atliq.streaming.silver_order_events
                                    ZORDER BY (event_ts);
                                    OPTIMIZE atliq.streaming.gold_revenue_5min
                                    ZORDER BY (window_start);
                                """,
                                http_path=SQL_WAREHOUSE_HTTP_PATH,
                            )

    refresh_daily_summary = DatabricksSqlOperator(
                                task_id="load_gold_daily_summary",
                                databricks_conn_id="databricks_default",
                                sql="""
                                    CREATE OR REPLACE TABLE atliq.streaming.gold_daily_summary AS
                                         SELECT
                                            DATE(event_ts) as event_date,
                                            COUNT_IF(event_type = 'order_placed') as orders_placed,
                                            COUNT_IF(event_type = 'payment_received') as orders_paid,
                                            COUNT_IF(event_type = 'order_cancelled') as orders_cancelled,
                                            SUM(
                                                CASE
                                                    WHEN event_type = 'payment_received'
                                                    THEN order_amount
                                                    ELSE 0
                                                END
                                            ) AS revenue

                                    FROM atliq.streaming.silver_order_events
                                    GROUP BY DATE(event_ts)
                                """,
                                http_path=SQL_WAREHOUSE_HTTP_PATH,
                            )
  
    check_fresh_events >> optimize_tables >> refresh_daily_summary
