SELECT s.id, s.customer_id
FROM sessions s
WHERE s.customer_id IN (21, 32, 36, 40) AND NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.event_type = 'click')
