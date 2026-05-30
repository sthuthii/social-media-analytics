cat > transform/trendpulse_dbt/models/marts/fct_engagement.sql << 'EOF'
-- marts/fct_engagement.sql
-- Engagement summary per subreddit

WITH posts AS (
    SELECT * FROM {{ ref('stg_posts') }}
)

SELECT
    subreddit,
    COUNT(*)                                AS total_posts,
    SUM(CASE WHEN is_viral THEN 1 ELSE 0 END) AS viral_posts,
    ROUND(AVG(upvotes), 2)                  AS avg_upvotes,
    ROUND(AVG(upvote_velocity), 2)          AS avg_velocity,
    ROUND(AVG(comment_ratio), 4)            AS avg_comment_ratio,
    ROUND(AVG(upvote_ratio), 2)             AS avg_upvote_ratio,
    MAX(upvotes)                            AS max_upvotes,
    ROUND(
        SUM(CASE WHEN is_viral THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                       AS viral_rate_pct
FROM posts
GROUP BY subreddit
ORDER BY viral_rate_pct DESC
EOF