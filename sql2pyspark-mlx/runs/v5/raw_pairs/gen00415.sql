WITH kept_keys AS (
    -- Set difference on keys
    SELECT page, session_id
    FROM web_events
    EXCEPT ALL
    SELECT page, session_id
    FROM web_events
    WHERE page NOT IN ('/cart', '/product', '/home')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM web_events s
    JOIN kept_keys k ON s.page = k.page AND s.session_id = k.session_id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    COUNT(*) FILTER (WHERE page = '/search') AS search_rows,
    COUNT(CASE WHEN page = '/home' THEN 1 END) AS home_count
FROM verified
GROUP BY quarter_start
ORDER BY quarter_start DESC NULLS LAST
