SELECT s.country, s.started_at, we.session_id FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id WHERE we.session_id = 39 ORDER BY we.session_id DESC
