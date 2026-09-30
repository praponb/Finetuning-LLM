select * from (select page, event_type, duration_sec from web_events) pivot (count(duration_sec) for event_type in ('add_to_cart', 'search')) order by page
