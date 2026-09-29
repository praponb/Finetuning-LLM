select
    id,
    session_id,
    event_time,
    lag(duration_sec) over w_ordered as prev_duration_sec,
    lead(duration_sec) over w_ordered as next_duration_sec,
    row_number() over w_ordered as seq_no,
    min(duration_sec) over w_part as page_min_duration_sec
from web_events
window w_ordered as (partition by session_id, page order by event_time, id), w_part as (partition by page);
