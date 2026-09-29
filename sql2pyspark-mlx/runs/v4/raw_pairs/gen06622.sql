SELECT s.id, s.country, s.started_at FROM sessions s WHERE NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.session_id IN (30, 34)) ORDER BY s.id DESC
