SELECT sessions.id, sessions.customer_id, sessions.browser FROM sessions WHERE EXISTS (SELECT 1 FROM web_events WHERE web_events.session_id = sessions.id AND web_events.duration_sec <> 245);
