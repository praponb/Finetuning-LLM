select *, datediff(current_date(), event_time) as days_since_event_time
from web_events
where event_type = 'page_view'
order by event_time desc
