SELECT s.id, s.device FROM sessions s WHERE s.started_at < '2025-07-21' AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.event_time >= '2023-11-01');
