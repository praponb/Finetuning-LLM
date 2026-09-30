SELECT page, ROUND((100 * SUM(CASE WHEN page = '/home' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_home
FROM web_events
WHERE (event_type <> 'page_view' AND session_id IN (9, 10, 13, 14)) OR event_time < '2024-09-15'
GROUP BY page
ORDER BY page
LIMIT 5;
