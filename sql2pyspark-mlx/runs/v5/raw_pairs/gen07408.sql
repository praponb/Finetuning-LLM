SELECT id, event_time, session_id, CONCAT(page, ' - ', event_type) AS page_event_type FROM web_events WHERE duration_sec > 325 AND session_id = 13 ORDER BY id DESC, page_event_type
