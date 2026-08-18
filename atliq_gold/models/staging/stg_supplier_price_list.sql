SELECT 
    product_id, 
    product_name, 
    supplier_name,
    supplier_cost,
    effective_date
FROM {{ source('silver', 'supplier_price_list') }}
