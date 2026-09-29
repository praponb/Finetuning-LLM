SELECT s.id, s.country, s.device FROM sessions s WHERE s.id IN (SELECT session_id FROM web_events WHERE event_time < '2024-05-01') ORDER BY s.device;
