SELECT 
    order_item_id, 
    order_id, 
    product_id, 
    quantity,
    item_price,
    created_at
FROM {{ source('silver', 'order_items') }}
