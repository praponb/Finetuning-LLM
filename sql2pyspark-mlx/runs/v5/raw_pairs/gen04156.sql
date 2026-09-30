SELECT id, duration_sec, page FROM web_events WHERE duration_sec > (SELECT MAX(duration_sec) FROM web_events) ORDER BY duration_sec DESC, id LIMIT 20
