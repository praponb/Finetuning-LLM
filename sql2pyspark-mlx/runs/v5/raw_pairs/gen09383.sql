SELECT s.id, s.started_at, s.country FROM sessions s WHERE s.device = 'tablet' AND NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id) ORDER BY s.country ASC
