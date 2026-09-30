select event_type, count(distinct duration_sec) unique_pages, max(event_time) last_event_time
from web_events
group by event_type
order by last_event_time
