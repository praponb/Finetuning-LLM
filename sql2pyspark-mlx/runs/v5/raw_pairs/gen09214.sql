SELECT id, event_time, duration_sec, event_type FROM web_events WHERE event_type != 'page_view' AND session_id = 33 AND event_type != 'page_view' ORDER BY event_type DESC, duration_sec
