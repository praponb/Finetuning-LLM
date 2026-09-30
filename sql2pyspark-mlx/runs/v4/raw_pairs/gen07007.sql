WITH flagged AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM web_events
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    id,
    event_type,
    page,
    event_time,
    duration_sec,
    is_new_session,
    session_no
FROM sessionized
ORDER BY event_type DESC
