SELECT event_type, DATE_TRUNC('MONTH', event_time) AS event_month, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range
FROM web_events
WHERE page IS NOT NULL AND page IN ('/search', '/home')
GROUP BY 1, 2
HAVING MAX(duration_sec) - MIN(duration_sec) >= 52
ORDER BY event_month;
