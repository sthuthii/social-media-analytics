cat > transform/trendpulse_dbt/models/marts/fct_viral_signals.sql << 'EOF'
-- marts/fct_viral_signals.sql
-- Feature-engineered table for ML virality prediction

WITH posts AS (
    SELECT * FROM {{ ref('stg_posts') }}
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
    is_viral,                               -- target label for ML
    scraped_at
FROM posts
EOF