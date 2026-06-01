
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        engagement_tier as value_field,
        count(*) as n_records

    from "trendpulse"."analytics"."stg_posts"
    group by engagement_tier

)

select *
from all_values
where value_field not in (
    'high','medium','low'
)



  
  
      
    ) dbt_internal_test