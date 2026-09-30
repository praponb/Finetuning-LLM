SELECT id, duration_sec, event_time
FROM web_events
WHERE duration_sec > (SELECT AVG(duration_sec) FROM web_events);
