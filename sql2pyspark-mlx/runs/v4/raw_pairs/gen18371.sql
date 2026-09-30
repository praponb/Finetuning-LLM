SELECT id, event_type FROM web_events WHERE page NOT IN ('/product', '/cart', '/checkout') UNION ALL SELECT id, event_type FROM web_events WHERE event_type = 'add_to_cart'
