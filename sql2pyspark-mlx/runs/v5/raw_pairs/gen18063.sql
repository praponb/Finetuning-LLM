SELECT
    id,
    session_id,
    page,
    duration_sec,
    AVG(duration_sec) OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_sec_avg_6,
    LAST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec
FROM web_events
