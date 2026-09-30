SELECT sessions.country, web_events.duration_sec FROM sessions LEFT JOIN web_events ON sessions.id = web_events.session_id
