WITH flagged AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM web_events
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    id,
    session_id,
    event_type,
    event_time,
    duration_sec,
    is_new_session,
    burst_id
FROM islands
