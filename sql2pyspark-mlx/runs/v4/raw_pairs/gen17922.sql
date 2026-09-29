SELECT id, session_id
FROM web_events
WHERE session_id IN (7, 36)
EXCEPT
SELECT id, session_id
FROM web_events
WHERE duration_sec BETWEEN 127 AND 425
