SELECT 
    payment_id, 
    order_id, 
    amount, 
    method
FROM {{ source('silver', 'payments') }}
