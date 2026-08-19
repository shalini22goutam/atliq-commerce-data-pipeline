
  
    
        create or replace table `atliq`.`gold`.`dim_products`
      
      
    using delta
  
      
      
      
      
      
      location 'abfss://lakehouse@atliqcommerce.dfs.core.windows.net/gold/dim_products'
      
      
      as
      with __dbt__cte__stg_products as (
SELECT 
    product_id, 
    product_name, 
    category, 
    unit_price
FROM `atliq`.`silver`.`products`
),  __dbt__cte__stg_supplier_price_list as (
SELECT 
    product_id, 
    product_name, 
    supplier_name,
    supplier_cost,
    effective_date
FROM `atliq`.`silver`.`supplier_price_list`
) -- models/gold/dim_product.sql

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.unit_price,
    s.supplier_cost,
    (p.unit_price - s.supplier_cost) AS unit_margin

FROM __dbt__cte__stg_products AS p

LEFT JOIN __dbt__cte__stg_supplier_price_list AS s
    ON p.product_id = s.product_id
  