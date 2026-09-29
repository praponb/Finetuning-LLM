select
    id,
    session_id,
    page,
    event_time,
    duration_sec,
    rank() over (partition by event_type, page order by duration_sec desc) as duration_sec_rank,
    ntile(4) over (order by duration_sec, id) as duration_sec_bucket,
    ntile(5) over (partition by session_id, page order by duration_sec desc, id) as duration_sec_bucket_2
from web_events
