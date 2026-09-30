SELECT sessions.id, sessions.country, sessions.customer_id
FROM sessions
WHERE EXISTS (SELECT 1 FROM web_events WHERE web_events.session_id = sessions.id)
