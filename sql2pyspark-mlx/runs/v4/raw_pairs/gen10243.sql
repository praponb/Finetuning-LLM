select id, duration_sec, session_id from web_events where to_date(event_time) >= '2023-12-29' union select id, duration_sec, session_id from web_events where event_type = 'add_to_cart'
