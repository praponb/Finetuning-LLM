SELECT event_type, COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS add_to_cart_count FROM web_events GROUP BY event_type ORDER BY add_to_cart_count DESC, event_type LIMIT 20;
