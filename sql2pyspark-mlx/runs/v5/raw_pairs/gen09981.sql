SELECT ROUND(AVG(duration_sec), 2) AS avg_duration_sec, MIN(event_time) AS first_event_time
FROM web_events
WHERE session_id IN (17, 31) AND page IN ('/home', '/cart')
