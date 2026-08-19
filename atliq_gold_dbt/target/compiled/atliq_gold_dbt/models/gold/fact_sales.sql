with __dbt__cte__stg_order_items as (
SELECT 
    order_item_id, 
    order_id, 
    product_id, 
    quantity,
    item_price
FROM `atliq`.`silver`.`order_items`
),  __dbt__cte__stg_orders as (
-- models/staging/stg_orders.sql
SELECT 
    order_id, 
    customer_id, 
    order_date, 
    status, 
    order_amount
FROM 
    `atliq`.`silver`.`orders`
) -- models/gold/fact_sales.sql
-- Grain: one row per order item





-- Grain: one row per order item

SELECT
    oi.order_item_id,
    o.order_id,
    o.customer_id,
    oi.product_id,
    o.order_date,
    oi.quantity,
    oi.item_price,
    oi.quantity * oi.item_price AS gross_revenue,
    o.status

FROM __dbt__cte__stg_order_items AS oi

INNER JOIN __dbt__cte__stg_orders AS o
    ON oi.order_id = o.order_id



WHERE oi.order_item_id > (
    SELECT COALESCE(MAX(order_item_id), 0)
    FROM `atliq`.`gold`.`fact_sales`
)

