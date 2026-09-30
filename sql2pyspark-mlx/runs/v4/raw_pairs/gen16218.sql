SELECT event_type, SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count FROM web_events GROUP BY event_type ORDER BY event_type
