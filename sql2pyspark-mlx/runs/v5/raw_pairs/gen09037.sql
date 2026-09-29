SELECT id, duration_sec, page, event_time FROM web_events WHERE duration_sec >= (SELECT MAX(duration_sec) FROM web_events WHERE duration_sec BETWEEN 253 AND 465) ORDER BY 4, id LIMIT 1;
