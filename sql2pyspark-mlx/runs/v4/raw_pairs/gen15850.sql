SELECT COUNT(CASE WHEN page = '/checkout' THEN 1 END) AS checkout_count, MAX(event_time) AS latest_event_time, SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count
FROM web_events
ORDER BY add_to_cart_count;
