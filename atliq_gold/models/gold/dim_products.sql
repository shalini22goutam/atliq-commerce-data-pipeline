-- models/gold/dim_product.sql

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.unit_price,
    s.supplier_cost,
    (p.unit_price - s.supplier_cost) AS unit_margin

FROM {{ ref('stg_products') }} AS p

LEFT JOIN {{ ref('stg_supplier_price_list') }} AS s
    ON p.product_id = s.product_id