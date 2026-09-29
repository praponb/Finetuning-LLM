select * from (select event_type, page, duration_sec from web_events) pivot (avg(duration_sec) for page in ('/checkout', '/cart')) order by event_type
