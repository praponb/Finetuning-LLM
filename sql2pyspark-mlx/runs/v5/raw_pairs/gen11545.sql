SELECT id, duration_sec, event_type, session_id FROM web_events WHERE duration_sec BETWEEN 191 AND 416 ORDER BY event_type DESC, duration_sec ASC, id ASC LIMIT 50;
