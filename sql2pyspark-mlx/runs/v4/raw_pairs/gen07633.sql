SELECT id, event_type, duration_sec, page FROM web_events WHERE page = '/cart' ORDER BY id DESC LIMIT 5
