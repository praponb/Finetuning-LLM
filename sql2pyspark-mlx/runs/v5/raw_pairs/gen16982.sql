select id, event_time, lower(page) page_lower
from web_events
where page not in ('/cart', '/search', '/home') and event_type != 'purchase' and to_date(event_time) < '2023-06-01'
