select
    id,
    event_type,
    page,
    event_time,
    duration_sec,
    lead(duration_sec, 1) over (partition by event_type, page order by event_time, id) as next_duration_sec
from web_events
order by id desc;
