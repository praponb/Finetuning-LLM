SELECT event_time, event_type, session_id FROM web_events WHERE event_type NOT IN ('add_to_cart', 'search') ORDER BY event_type DESC
