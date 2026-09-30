SELECT sessions.id, sessions.country, sessions.customer_id, sessions.started_at FROM sessions WHERE sessions.id IN (SELECT session_id FROM web_events WHERE session_id IN (7, 11, 23, 27))
