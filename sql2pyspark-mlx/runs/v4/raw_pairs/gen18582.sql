SELECT id, duration_sec, session_id FROM web_events WHERE page IN ('/search', '/checkout', '/cart') UNION SELECT id, duration_sec, session_id FROM web_events WHERE duration_sec <= 173
