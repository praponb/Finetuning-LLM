SELECT id, duration_sec, event_type FROM web_events WHERE TO_DATE(event_time) >= '2023-12-01' EXCEPT SELECT id, duration_sec, event_type FROM web_events WHERE page IN ('/home', '/cart', '/checkout');
