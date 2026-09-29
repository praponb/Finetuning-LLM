SELECT s.id, s.customer_id, s.country FROM sessions s WHERE s.customer_id IS NULL AND NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.duration_sec < 179)
