SELECT
    id,
    page,
    event_type,
    event_time,
    duration_sec,
    LEAD(duration_sec, 1) OVER (PARTITION BY page, event_type ORDER BY event_time, id) next_duration_sec,
    ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time DESC, id) row_num
FROM web_events
WHERE event_time < '2023-07-01';
