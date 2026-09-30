SELECT id, duration_sec, session_id, event_time FROM web_events WHERE duration_sec > (SELECT AVG(duration_sec) FROM web_events) ORDER BY session_id DESC;
