SELECT web_events.id, web_events.session_id FROM web_events WHERE web_events.session_id NOT IN (SELECT id FROM sessions WHERE started_at < '2024-12-01') ORDER BY web_events.session_id DESC
