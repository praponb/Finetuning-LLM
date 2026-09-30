select
    id,
    session_id,
    duration_sec,
    event_type,
    event_time,
    row_number() over (partition by session_id order by duration_sec, id) as duration_sec_row,
    lead(duration_sec, 2) over (partition by event_type order by event_time, id) as next_duration_sec
from web_events
order by duration_sec_row desc, id desc;
