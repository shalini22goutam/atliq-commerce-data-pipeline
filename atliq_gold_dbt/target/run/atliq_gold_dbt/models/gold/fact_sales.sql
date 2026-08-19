-- back compat for old kwarg name
  
  
  
  
  
  
      
          
          
      
  

    merge
    into
        `atliq`.`gold`.`fact_sales` as DBT_INTERNAL_DEST
    using
        `fact_sales__dbt_tmp` as DBT_INTERNAL_SOURCE
    on
        
              DBT_INTERNAL_SOURCE.`order_item_id` <=> DBT_INTERNAL_DEST.`order_item_id`
          
    when matched
        then update set
            *
    when not matched
        then insert
            *
