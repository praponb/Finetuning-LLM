with remaining_keys as (
    select event_type, session_id
    from web_events
    intersect all
    select event_type, session_id
    from web_events
    where session_id in (4, 18)
),
reconciled as (
    select s.*
    from web_events s
    inner join remaining_keys k on s.event_type = k.event_type and s.session_id = k.session_id
)
select
    session_id,
    event_type,
    page,
    event_time,
    duration_sec
from reconciled
order by event_time desc
