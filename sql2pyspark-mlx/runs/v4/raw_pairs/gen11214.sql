SELECT page, COUNT(*) AS row_count, COUNT(DISTINCT duration_sec) AS unique_event_types
FROM web_events
WHERE page = '/product'
GROUP BY page
HAVING COUNT(*) < 1
