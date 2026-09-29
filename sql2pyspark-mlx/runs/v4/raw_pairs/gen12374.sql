SELECT id, event_type
FROM web_events
WHERE session_id = 40
EXCEPT
SELECT id, event_type
FROM web_events
WHERE session_id IN (2, 21, 24)
