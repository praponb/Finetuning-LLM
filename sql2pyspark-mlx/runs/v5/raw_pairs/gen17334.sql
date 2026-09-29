SELECT web_events.id, web_events.session_id, web_events.duration_sec FROM web_events WHERE web_events.session_id NOT IN (SELECT id FROM sessions WHERE device IN ('tablet', 'mobile'))
