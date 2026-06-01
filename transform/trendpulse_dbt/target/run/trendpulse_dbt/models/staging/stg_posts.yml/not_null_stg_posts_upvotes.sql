
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select upvotes
from "trendpulse"."analytics"."stg_posts"
where upvotes is null



  
  
      
    ) dbt_internal_test