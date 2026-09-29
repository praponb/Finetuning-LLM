SELECT
    event_type,
    page,
    COUNT(*) num_web_events,
    MAX(event_time) latest_event_time
FROM web_events
WHERE duration_sec = 367
GROUP BY event_type, page
ORDER BY latest_event_time DESC
