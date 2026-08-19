-- models/staging/stg_orders.sql
SELECT 
    order_id, 
    customer_id, 
    order_date, 
    status, 
    order_amount
FROM 
    `atliq`.`silver`.`orders`