SELECT s.started_at, s.device, s.browser, we.duration_sec, we.event_type FROM sessions s INNER JOIN web_events we ON we.session_id = s.id;
