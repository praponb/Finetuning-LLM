select
    id,
    event_time,
    duration_sec,
    sum(duration_sec) over (order by event_time, id) as running_duration_sec
from web_events
where page = '/product'
order by event_time
