
  
    
        create or replace table `atliq`.`gold`.`dim_customers`
      
      
    using delta
  
      
      
      
      
      
      location 'abfss://lakehouse@atliqcommerce.dfs.core.windows.net/gold/dim_customers'
      
      
      as
      with __dbt__cte__stg_customers as (
SELECT 
    customer_id, 
    customer_name, 
    email, 
    city,
    signup_date
FROM `atliq`.`silver`.`customers`
) -- models/gold/dim_customer.sql

SELECT
    customer_id,
    customer_name,
    email,
    city,
    signup_date

FROM __dbt__cte__stg_customers
  