WITH gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM web_events
),
sessions_web_events AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.page,
    s.event_time,
    s.burst_id,
    (SELECT SUM(t2.duration_sec) FROM web_events t2 WHERE t2.session_id = s.session_id) AS sum_web_event_duration_sec
FROM sessions_web_events s
ORDER BY event_time DESC NULLS LAST;
