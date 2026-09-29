SELECT
    id,
    page,
    event_time,
    event_type,
    duration_sec,
    ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time, id) AS rn,
    SUM(duration_sec) OVER (PARTITION BY event_type) AS event_type_sum_duration_sec
FROM web_events
WHERE duration_sec <= 358
ORDER BY event_time
