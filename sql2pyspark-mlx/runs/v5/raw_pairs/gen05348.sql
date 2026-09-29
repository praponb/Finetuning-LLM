SELECT s.device, we.event_type, we.session_id FROM sessions s LEFT OUTER JOIN web_events we ON s.id = we.session_id;
