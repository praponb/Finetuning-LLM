WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        TRIM(page) AS clean_page,
        CONCAT(page, '/', event_type) AS page_event_type,
        COALESCE(page, 'OTHER') AS page_filled
    FROM web_events
    WHERE event_time >= TO_TIMESTAMP('2023-04-09 06:00:00')
      AND duration_sec = 360
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    island_id,
    event_type,
    MIN(event_time) AS first_seen,
    COUNT(*) FILTER (WHERE page = '/checkout') AS checkout_rows
FROM sessions_web_events
GROUP BY ROLLUP (island_id, event_type)
ORDER BY checkout_rows NULLS FIRST;
