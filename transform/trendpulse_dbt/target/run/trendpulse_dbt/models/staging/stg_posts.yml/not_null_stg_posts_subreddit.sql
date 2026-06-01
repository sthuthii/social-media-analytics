
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select subreddit
from "trendpulse"."analytics"."stg_posts"
where subreddit is null



  
  
      
    ) dbt_internal_test