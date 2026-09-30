select id, page, event_time from web_events where page not in ('/checkout', '/product') order by event_time desc, page desc;
