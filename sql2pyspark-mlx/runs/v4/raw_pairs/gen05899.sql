SELECT *, LENGTH(event_type) AS event_type_length, CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END AS is_add_to_cart FROM web_events;
