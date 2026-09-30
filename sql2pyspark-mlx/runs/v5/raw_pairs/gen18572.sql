SELECT
    id,
    session_id,
    event_time,
    duration_sec,
    LEAD(duration_sec, 1) OVER (PARTITION BY session_id ORDER BY event_time, id) AS next_duration_sec
FROM web_events
WHERE event_time >= '2024-01-01';
