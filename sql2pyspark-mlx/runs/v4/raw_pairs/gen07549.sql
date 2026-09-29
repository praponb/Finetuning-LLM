SELECT duration_sec, UPPER(event_type) event_type_upper FROM web_events WHERE event_type IN ('add_to_cart', 'page_view')
