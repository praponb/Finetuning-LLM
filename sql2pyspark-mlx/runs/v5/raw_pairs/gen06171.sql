SELECT event_type, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
FROM web_events
GROUP BY event_type
HAVING ROUND(STDDEV(duration_sec), 2) > 52
