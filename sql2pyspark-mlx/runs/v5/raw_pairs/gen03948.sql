SELECT id, duration_sec, event_type, event_time FROM web_events WHERE duration_sec >= (SELECT AVG(duration_sec) FROM web_events) ORDER BY event_time
