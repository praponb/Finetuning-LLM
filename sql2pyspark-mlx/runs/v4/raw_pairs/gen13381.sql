SELECT s.started_at, we.event_time, we.event_type, we.session_id FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id WHERE we.page != '/home';
