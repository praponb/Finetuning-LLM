SELECT we.id, we.page, we.event_type, we.session_id, s.country, s.browser, s.started_at FROM web_events we INNER JOIN sessions s ON we.session_id = s.id;
