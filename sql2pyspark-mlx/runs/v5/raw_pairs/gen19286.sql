SELECT id, event_type, duration_sec, CONCAT_WS(',', event_type, page) event_type_page_key FROM web_events WHERE session_id IN (8, 34) ORDER BY duration_sec DESC, id LIMIT 1;
