{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    incremental_strategy='merge'
) }}

SELECT
    oi.order_item_id,
    o.order_id,
    dc.customer_key,
    dp.product_key,
    o.customer_id,      -- kept for traceability/debugging
    oi.product_id,       -- kept for traceability/debugging
    o.order_date,
    oi.quantity,
    oi.item_price,
    oi.quantity * oi.item_price AS revenue,
    o.status,
    o.updated_at

FROM {{ ref('stg_order_items') }} AS oi

INNER JOIN {{ ref('stg_orders') }} AS o
    ON oi.order_id = o.order_id

LEFT JOIN {{ ref('dim_customers') }} AS dc
    ON o.customer_id = dc.customer_id

LEFT JOIN {{ ref('dim_products') }} AS dp
    ON oi.product_id = dp.product_id

{% if is_incremental() %}

WHERE o.updated_at > (
    SELECT COALESCE(MAX(updated_at), '1900-01-01')
    FROM {{ this }}
)

{% endif %}