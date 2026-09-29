select
    id,
    session_id,
    page,
    event_time,
    duration_sec,
    lead(duration_sec, 2) over (partition by session_id, page order by event_time, id) next_duration_sec,
    sum(duration_sec) over (partition by page order by event_time, id) running_duration_sec
from web_events
where page <> '/cart'
