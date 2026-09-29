select max(event_time) as latest_event_time
from web_events
where duration_sec between 341 and 462
order by latest_event_time;
