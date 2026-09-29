WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, HOUR(started_at) AS started_at_hour
    FROM sessions
    WHERE started_at >= DATE '2024-03-18'
      AND browser IS NOT NULL
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', started_at) AS week_start,
    CASE WHEN started_at_hour >= 86 THEN 'LARGE' ELSE 'SMALL' END AS started_at_hour_tier,
    MIN(started_at) AS first_started_at,
    MIN(started_at) AS first_seen,
    ROUND(AVG(started_at_hour), 2) AS avg_started_at_hour,
    GROUPING_ID() AS grouping_id
FROM base
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', started_at), CASE WHEN started_at_hour >= 86 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('WEEK', started_at)), ())
ORDER BY started_at_hour_tier
