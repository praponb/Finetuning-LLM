SELECT s.started_at, s.device, we.session_id FROM sessions s LEFT JOIN web_events we ON s.id = we.session_id;
