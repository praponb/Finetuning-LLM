SELECT event_type, session_id
FROM web_events
INTERSECT ALL
SELECT event_type, session_id
FROM web_events
WHERE duration_sec BETWEEN 122 AND 307;
