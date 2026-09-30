SELECT id, event_type
FROM web_events
WHERE HOUR(event_time) BETWEEN 9 AND 13
UNION
SELECT id, event_type
FROM web_events
WHERE duration_sec = 249
ORDER BY id;
