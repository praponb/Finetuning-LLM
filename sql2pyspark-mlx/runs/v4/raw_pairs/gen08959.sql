SELECT id, duration_sec FROM web_events WHERE duration_sec < (SELECT MIN(duration_sec) FROM web_events WHERE page IS NULL) ORDER BY duration_sec
