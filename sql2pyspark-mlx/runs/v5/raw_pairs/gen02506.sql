SELECT MIN(duration_sec) AS min_duration_sec
FROM web_events
WHERE session_id = 39 AND (HOUR(event_time) BETWEEN 8 AND 15 OR TO_DATE(event_time) >= '2024-05-01')
ORDER BY min_duration_sec;
