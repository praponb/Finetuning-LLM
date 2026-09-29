SELECT MONTH(event_time) AS event_month, COUNT(*) AS total_count, SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count FROM web_events GROUP BY event_month;
