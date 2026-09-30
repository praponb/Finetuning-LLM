WITH matched_keys AS (
    SELECT country, browser
    FROM sessions
    EXCEPT DISTINCT
    SELECT country, browser
    FROM sessions
    WHERE device = 'tablet'
),
verified AS (
    SELECT s.*
    FROM sessions s
    INNER JOIN matched_keys k ON s.country = k.country AND s.browser = k.browser
)
SELECT
    DATE_TRUNC('WEEK', started_at) AS week_start,
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    COUNT(CASE WHEN device = 'desktop' THEN 1 END) AS desktop_count,
    MIN(started_at) AS first_seen,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    GROUPING_ID() AS gid
FROM verified
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', started_at), DATE_TRUNC('QUARTER', started_at)), (DATE_TRUNC('WEEK', started_at)))
