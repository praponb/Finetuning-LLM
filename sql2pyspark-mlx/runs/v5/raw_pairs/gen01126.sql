SELECT event_type FROM web_events WHERE session_id = 26 AND (event_type = 'add_to_cart' OR duration_sec >= 240) ORDER BY event_type;
