SELECT s.customer_id, s.started_at, we.event_time FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id WHERE s.browser IN ('Safari', 'Chrome')
