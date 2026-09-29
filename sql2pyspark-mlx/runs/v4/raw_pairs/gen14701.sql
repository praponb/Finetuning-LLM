with kept_keys as (
    select session_id
    from web_events
    intersect
    select session_id
    from web_events
    where event_type = 'click'
),
verified as (
    select s.*
    from web_events s
    inner join kept_keys k on s.session_id = k.session_id
)
select
    s.id,
    s.session_id,
    s.event_type,
    s.page,
    s.event_time,
    s.duration_sec,
    (select max(t2.duration_sec) from web_events t2 where t2.session_id = s.session_id) as max_web_event_duration_sec
from verified s
order by event_time desc nulls first
