SELECT s.id, s.started_at, s.customer_id FROM sessions s WHERE s.id IN (SELECT session_id FROM web_events WHERE event_time < '2023-05-17')
