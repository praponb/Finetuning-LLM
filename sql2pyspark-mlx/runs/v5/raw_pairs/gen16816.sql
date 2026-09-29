SELECT s.id, s.browser, s.started_at, s.customer_id FROM sessions s WHERE NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id) ORDER BY browser, id LIMIT 10;
