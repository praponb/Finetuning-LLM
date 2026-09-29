SELECT
    id,
    event_time,
    duration_sec,
    duration_sec - LAG(duration_sec, 2, 0) OVER (ORDER BY event_time, id) AS duration_sec_change
FROM web_events
