SELECT s.id, s.customer_id, s.started_at, s.browser FROM sessions s WHERE s.id IN (SELECT session_id FROM web_events)
