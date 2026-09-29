select
    id,
    session_id,
    event_time,
    duration_sec,
    sum(duration_sec) over (partition by session_id order by event_time, id) as running_duration_sec
from web_events
where event_time < '2023-12-07'
order by 1 desc
