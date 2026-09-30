SELECT duration_sec
FROM web_events
WHERE event_type != 'page_view'
UNION
SELECT duration_sec
FROM web_events
WHERE event_time < '2025-10-09';
