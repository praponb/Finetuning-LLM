SELECT s.device, s.started_at, s.country, we.session_id FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id WHERE we.page IN ('/search', '/cart', '/checkout');
