SELECT
    id,
    event_type,
    event_time,
    duration_sec,
    LAG(duration_sec, 2) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_duration_sec
FROM web_events
