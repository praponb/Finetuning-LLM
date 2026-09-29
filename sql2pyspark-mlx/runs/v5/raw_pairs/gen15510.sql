SELECT id, session_id, page FROM web_events WHERE session_id IN (5, 8, 9, 13) UNION SELECT id, session_id, page FROM web_events WHERE event_type = 'add_to_cart' ORDER BY page
