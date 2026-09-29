select * from web_events where session_id in (11, 26, 27) and (hour(event_time) between 10 and 13 or event_type = 'add_to_cart');
