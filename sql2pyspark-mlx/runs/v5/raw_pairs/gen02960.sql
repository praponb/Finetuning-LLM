SELECT event_type, COUNT(CASE WHEN page = '/home' THEN 1 END) AS home_count, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
FROM web_events
WHERE event_time < '2023-11-01' AND event_time >= '2024-01-01'
GROUP BY event_type
HAVING COUNT(CASE WHEN page = '/home' THEN 1 END) < 5
ORDER BY stddev_duration_sec ASC
