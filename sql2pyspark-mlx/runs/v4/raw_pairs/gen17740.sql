SELECT id, event_type, event_time, duration_sec, FLOOR(duration_sec) AS duration_sec_whole FROM web_events WHERE session_id = 11 AND page = '/checkout' ORDER BY event_type, id LIMIT 1
