SELECT s.browser, s.started_at, s.customer_id, we.event_time, we.event_type FROM sessions s INNER JOIN web_events we ON we.session_id = s.id WHERE we.session_id IN (18, 21);
