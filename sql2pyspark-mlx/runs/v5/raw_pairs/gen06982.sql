with valid as (
    select *, length(event_type) as event_type_length
    from web_events
    where event_type in ('search', 'purchase', 'click')
)
select
    s.device,
    s.browser,
    e.event_type,
    count(*) as rows_total,
    sum(e.duration_sec) as total_duration_sec,
    min(e.event_type_length) as min_event_type_length,
    grouping_id() as gid
from valid e
left join sessions s on e.session_id = s.id
group by rollup (s.device, s.browser, e.event_type)
