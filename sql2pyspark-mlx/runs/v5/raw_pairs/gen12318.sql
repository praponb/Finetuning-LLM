SELECT
    id,
    event_type,
    event_time,
    duration_sec,
    LAST_VALUE(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec
FROM web_events
WHERE page = '/checkout';
