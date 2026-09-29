SELECT s.country, s.started_at, s.browser, we.page, we.session_id FROM sessions s INNER JOIN web_events we ON s.id = we.session_id WHERE we.duration_sec <> 238;
