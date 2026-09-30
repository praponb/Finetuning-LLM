SELECT page, SUM(CASE WHEN event_type = 'add_to_cart' THEN duration_sec ELSE 0 END) AS add_to_cart_duration_sec
FROM web_events
GROUP BY 1
