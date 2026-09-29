select duration_sec, lower(event_type) as event_type_lower
from web_events
where page not in ('/checkout', '/home')
order by event_type_lower
