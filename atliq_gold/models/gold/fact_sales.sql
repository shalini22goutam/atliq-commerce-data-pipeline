-- models/gold/fact_sales.sql
-- Grain: one row per order item

{{ config(
    materialized='incremental',
    unique_key='order_item_id'
) }}

SELECT
    oi.order_item_id,
    o.order_id,
    o.customer_id,
    oi.product_id,
    o.order_date,
    oi.quantity,
    oi.item_price,
    oi.quantity * oi.item_price AS gross_revenue,
    o.status,
    oi.created_at 

FROM {{ ref('stg_order_items') }} AS oi

INNER JOIN {{ ref('stg_orders') }} AS o
    ON oi.order_id = o.order_id

{% if is_incremental() %}

WHERE oi.created_at > (
    SELECT MAX(watermark)
    FROM {{ this }}
)

{% endif %}

