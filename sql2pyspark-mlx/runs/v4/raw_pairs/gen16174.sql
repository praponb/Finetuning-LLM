select event_type, page from web_events where page = '/cart' union select event_type, page from web_events where duration_sec < 349
