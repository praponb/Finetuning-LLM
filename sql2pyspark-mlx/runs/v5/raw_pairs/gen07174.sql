SELECT
    id,
    session_id,
    event_type,
    event_time,
    duration_sec,
    duration_sec - LAG(duration_sec, 1) OVER (PARTITION BY session_id, event_type ORDER BY event_time, id) AS duration_sec_change
FROM web_events
WHERE event_type = 'click'
