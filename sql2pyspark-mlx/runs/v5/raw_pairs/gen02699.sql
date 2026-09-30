select * from (select event_type, page, duration_sec from web_events) pivot (sum(duration_sec) for page in ('/search', '/product')) order by event_type
