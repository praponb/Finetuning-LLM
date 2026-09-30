SELECT event_type, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
FROM web_events
WHERE event_type = 'add_to_cart' AND duration_sec = 171
GROUP BY event_type
ORDER BY event_type
LIMIT 10
