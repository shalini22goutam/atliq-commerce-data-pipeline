SELECT 
    customer_id, 
    customer_name, 
    email, 
    city,
    signup_date
FROM {{ source('silver', 'customers') }}
