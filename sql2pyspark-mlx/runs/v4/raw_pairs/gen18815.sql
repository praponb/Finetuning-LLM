SELECT session_id, page, CONCAT_WS('-', event_type, page) AS event_type_page_key
FROM web_events
WHERE session_id IN (4, 25, 31, 39);
