SELECT s.started_at, we.page, we.event_time FROM sessions s LEFT OUTER JOIN web_events we ON s.id = we.session_id ORDER BY s.started_at DESC;
