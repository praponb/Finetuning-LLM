SELECT id, session_id, CONCAT_WS('-', page, event_type) AS page_event_type_key FROM web_events WHERE duration_sec <= 248 AND session_id IN (21, 22, 28, 39) AND event_type = 'page_view';
