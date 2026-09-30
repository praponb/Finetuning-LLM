SELECT event_type, SUM(CASE WHEN page = '/search' THEN 1 ELSE 0 END) AS search_count, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
FROM web_events
WHERE event_time < '2023-12-20'
GROUP BY 1
