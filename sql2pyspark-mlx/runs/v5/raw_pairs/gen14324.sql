SELECT event_type, DATE_FORMAT(event_time, 'yyyy-MM') event_month, SUM(CASE WHEN event_type = 'search' THEN duration_sec ELSE 0 END) search_duration_sec
FROM web_events
GROUP BY event_type, DATE_FORMAT(event_time, 'yyyy-MM')
ORDER BY search_duration_sec
