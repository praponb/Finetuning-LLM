SELECT s.id, s.device, s.customer_id
FROM sessions s
WHERE HOUR(s.started_at) BETWEEN 10 AND 18 AND s.id IN (SELECT session_id FROM web_events WHERE page = '/product')
