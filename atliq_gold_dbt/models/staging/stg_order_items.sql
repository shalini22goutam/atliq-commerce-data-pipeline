SELECT 
    order_item_id, 
    order_id, 
    product_id, 
    quantity,
    item_price
FROM {{ source('silver', 'order_items') }}
