SELECT web_events.id, web_events.duration_sec, web_events.session_id, web_events.event_type FROM web_events WHERE web_events.session_id NOT IN (SELECT id FROM sessions WHERE browser = 'Edge');
