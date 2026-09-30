WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time < s.started_at
        )
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', started_at) AS year_start,
    country,
    COUNT(CASE WHEN device = 'desktop' THEN 1 END) AS desktop_count,
    MIN(started_at) AS first_seen,
    MAX(started_at) AS ended_at,
    GROUPING(DATE_TRUNC('YEAR', started_at)) AS grouping_level
FROM kept
GROUP BY ROLLUP (DATE_TRUNC('YEAR', started_at), country)
ORDER BY desktop_count DESC NULLS LAST;
