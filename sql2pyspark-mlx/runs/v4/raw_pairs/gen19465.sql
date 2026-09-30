SELECT event_type, SUM(duration_sec) AS total_duration_sec, SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END) AS search_count
FROM web_events
GROUP BY event_type
HAVING SUM(duration_sec) >= 2100
