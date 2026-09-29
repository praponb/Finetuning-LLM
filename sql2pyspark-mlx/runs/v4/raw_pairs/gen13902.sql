SELECT id, duration_sec, event_time
FROM web_events
WHERE duration_sec = (SELECT MIN(duration_sec) FROM web_events)
ORDER BY event_time, id
LIMIT 1
