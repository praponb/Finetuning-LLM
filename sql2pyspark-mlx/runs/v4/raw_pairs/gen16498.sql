SELECT s.id, s.country, s.started_at, s.customer_id FROM sessions s WHERE NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.session_id = 15) ORDER BY s.country
