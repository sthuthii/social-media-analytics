cat > transform/trendpulse_dbt/models/staging/stg_posts.sql << 'EOF'
-- staging/stg_posts.sql
-- Cleans and standardises raw Reddit posts

WITH source AS (
    SELECT * FROM raw_posts
),

cleaned AS (
    SELECT
        post_id,
        TRIM(title)                          AS title,
        LOWER(subreddit)                     AS subreddit,
        upvotes,
        upvote_ratio,
        num_comments,
        awards,
        created_minutes_ago,
        upvote_velocity,
        comment_ratio,
        author_karma,
        is_self_post,
        hour_posted,
        day_of_week,
        is_viral,
        scraped_at,

        -- Derived fields
        CASE
            WHEN hour_posted BETWEEN 6  AND 11 THEN 'morning'
            WHEN hour_posted BETWEEN 12 AND 17 THEN 'afternoon'
            WHEN hour_posted BETWEEN 18 AND 22 THEN 'evening'
            ELSE 'night'
        END                                  AS time_of_day,

        CASE
            WHEN day_of_week IN (5, 6)       THEN true
            ELSE false
        END                                  AS is_weekend,

        CASE
            WHEN upvotes >= 1000             THEN 'high'
            WHEN upvotes >= 100              THEN 'medium'
            ELSE 'low'
        END                                  AS engagement_tier

    FROM source
    WHERE post_id IS NOT NULL
      AND upvotes >= 0
)

SELECT * FROM cleaned
EOF