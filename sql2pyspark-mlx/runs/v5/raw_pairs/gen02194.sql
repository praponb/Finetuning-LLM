SELECT id, event_time
FROM web_events
WHERE TO_DATE(event_time) < '2025-06-01'
