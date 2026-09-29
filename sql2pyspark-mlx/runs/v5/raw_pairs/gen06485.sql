SELECT s.id, s.device FROM sessions s WHERE EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.session_id = 19)
