WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) AS prev_event_time
    FROM web_events
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    CASE WHEN island_id >= 8 THEN 'LARGE' ELSE 'SMALL' END AS island_id_tier,
    COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) AS page_view_count,
    ROUND(AVG(island_id), 2) AS avg_island_id
FROM sessions_web_events
GROUP BY DATE_TRUNC('QUARTER', event_time), CASE WHEN island_id >= 8 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY page_view_count
