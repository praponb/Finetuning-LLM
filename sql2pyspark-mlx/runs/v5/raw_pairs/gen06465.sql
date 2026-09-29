WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM web_events
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    page,
    session_id,
    COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_view_rows,
    MAX(is_new_session) FILTER (WHERE event_type = 'page_view') AS page_view_max_is_new_session,
    MAX(event_time) AS ended_at,
    SUM(CASE WHEN event_type = 'search' THEN burst_id ELSE 0 END) AS search_burst_id,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min
FROM sessions_web_events
GROUP BY 1, 2
ORDER BY duration_min DESC;
