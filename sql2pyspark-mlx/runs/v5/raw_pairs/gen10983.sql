SELECT *
FROM web_events
WHERE event_time >= '2025-05-11' AND event_type = 'page_view'
ORDER BY event_time
