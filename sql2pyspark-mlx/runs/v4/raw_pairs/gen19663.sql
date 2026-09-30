SELECT id, duration_sec, event_time, page FROM web_events WHERE duration_sec > (SELECT AVG(duration_sec) FROM web_events) ORDER BY id DESC
