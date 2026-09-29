SELECT a.id, a.session_id, a.duration_sec, a.event_type FROM web_events a WHERE a.duration_sec = (SELECT MIN(b.duration_sec) FROM web_events b WHERE b.session_id = a.session_id);
