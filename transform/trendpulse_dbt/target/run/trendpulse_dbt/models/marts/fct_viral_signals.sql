
  
    

  create  table "trendpulse"."analytics"."fct_viral_signals__dbt_tmp"
  
  
    as
  
  (
    WITH posts AS (
    SELECT * FROM "trendpulse"."analytics"."stg_posts"
)

SELECT
    post_id,
    title,
    subreddit,
    upvotes,
    upvote_velocity,
    comment_ratio,
    upvote_ratio,
    awards,
    author_karma,
    created_minutes_ago,
    hour_posted,
    day_of_week,
    is_weekend,
    time_of_day,
    engagement_tier,
    is_self_post,
    is_viral,
    scraped_at
FROM posts
  );
  