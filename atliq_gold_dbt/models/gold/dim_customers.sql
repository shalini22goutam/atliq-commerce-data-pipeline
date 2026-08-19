-- models/gold/dim_customer.sql

SELECT
    customer_id,
    customer_name,
    email,
    city,
    signup_date

FROM {{ ref('stg_customers') }}