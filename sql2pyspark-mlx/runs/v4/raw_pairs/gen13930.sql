SELECT id, page, session_id FROM web_events WHERE event_type = 'add_to_cart' UNION SELECT id, page, session_id FROM web_events WHERE event_type = 'purchase' ORDER BY id;
