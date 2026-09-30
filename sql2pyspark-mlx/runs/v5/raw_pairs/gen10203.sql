select
    id,
    page,
    event_type,
    dayofweek(event_time) as event_time_dayofweek
from web_events
where session_id = 10;
