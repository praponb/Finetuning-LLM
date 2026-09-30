SELECT id, event_type
FROM web_events
WHERE TO_DATE(event_time) < '2025-01-01'
UNION ALL
SELECT id, event_type
FROM web_events
WHERE page = '/product'
