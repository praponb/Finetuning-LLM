SELECT s.country, s.device, we.session_id, we.duration_sec FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id;
