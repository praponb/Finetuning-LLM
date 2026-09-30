select id, event_time, duration_sec
from web_events
where event_type not in ('click', 'purchase', 'page_view')
