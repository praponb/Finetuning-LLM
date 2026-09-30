SELECT s.country, we.session_id FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id ORDER BY 1
