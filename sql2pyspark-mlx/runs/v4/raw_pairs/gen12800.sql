SELECT id, event_type, duration_sec, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec, id) AS rn FROM web_events WHERE page IN ('/search', '/checkout', '/product')
