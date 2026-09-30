SELECT event_type, SUM(duration_sec) AS total_duration_sec, SUM(SUM(duration_sec)) OVER (ORDER BY event_type) AS cumulative_duration_sec
FROM web_events
WHERE page IN ('/home', '/product')
GROUP BY event_type
