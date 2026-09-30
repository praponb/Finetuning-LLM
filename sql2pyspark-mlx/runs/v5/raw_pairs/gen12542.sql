select
    id,
    event_type,
    event_time,
    duration_sec,
    session_id,
    round(avg(duration_sec) over (partition by event_type order by event_time, id rows between 3 preceding and current row), 2) as duration_sec_moving_avg_4,
    row_number() over (partition by session_id, event_type order by event_time, id) as rn
from web_events
