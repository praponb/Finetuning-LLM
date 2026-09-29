select hour(event_time) as event_hour, min(duration_sec) as min_duration_sec
from web_events
where hour(event_time) between 12 and 18 and event_type = 'search'
group by event_hour
having min(duration_sec) >= 400
