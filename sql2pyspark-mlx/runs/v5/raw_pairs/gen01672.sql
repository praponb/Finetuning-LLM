SELECT id, event_type, page, session_id, CONCAT_WS('-', page, event_type) AS page_event_type_key FROM web_events WHERE duration_sec >= 241 AND event_type = 'search' AND session_id = 37;
