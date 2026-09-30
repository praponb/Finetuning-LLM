SELECT id, session_id, event_time
FROM web_events
ORDER BY event_time, session_id DESC, id
LIMIT 5;
