SELECT s.id, s.device, s.country, s.started_at FROM sessions s WHERE s.id IN (SELECT session_id FROM web_events WHERE duration_sec < 158)
