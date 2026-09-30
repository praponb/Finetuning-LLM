WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        session_id,
        page,
        duration_sec
    FROM (
        SELECT *, RANK() OVER (PARTITION BY page ORDER BY event_time) AS latest_rank
        FROM web_events
    ) ranked
    WHERE latest_rank = 1
)
-- Grouped metrics
SELECT
    session_id,
    COUNT(CASE WHEN page = '/home' THEN 1 END) AS home_count,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    COUNT(*) FILTER (WHERE page = '/search') AS search_rows,
    COUNT(*) FILTER (WHERE page = '/search') AS search_rows_2,
    COUNT(*) FILTER (WHERE page = '/home') AS home_rows
FROM deduped
GROUP BY session_id;
