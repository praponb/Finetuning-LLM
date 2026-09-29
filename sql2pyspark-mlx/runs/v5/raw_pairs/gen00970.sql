SELECT s.customer_id, s.browser, we.duration_sec, we.session_id, we.event_time FROM sessions s INNER JOIN web_events we ON s.id = we.session_id;
