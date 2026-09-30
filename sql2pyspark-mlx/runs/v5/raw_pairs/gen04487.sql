select event_type from web_events where page in ('/product', '/checkout', '/home') union select event_type from web_events where event_type = 'add_to_cart' order by event_type;
