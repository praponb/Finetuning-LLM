SELECT sessions.id, sessions.started_at
FROM sessions
WHERE sessions.id IN (SELECT session_id FROM web_events);
