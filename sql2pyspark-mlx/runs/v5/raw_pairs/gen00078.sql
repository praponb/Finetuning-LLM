SELECT event_type, DATE_FORMAT(event_time, 'yyyy-MM') event_month, ROUND((100 * SUM(CASE WHEN page = '/search' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_search
FROM web_events
WHERE (session_id = 17 AND duration_sec > 197) OR page = '/search'
GROUP BY event_type, DATE_FORMAT(event_time, 'yyyy-MM')
ORDER BY pct_search DESC;
