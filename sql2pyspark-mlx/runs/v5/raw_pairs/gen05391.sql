with metrics as (
    select
        *,
        last_value(duration_sec, true) over (order by duration_sec, id rows between unbounded preceding and current row) last_known_duration_sec,
        ntile(10) over (partition by page, session_id order by duration_sec desc, id) duration_sec_bucket,
        count(duration_sec) over (partition by event_type order by cast(event_time as bigint) range between 7776000 preceding and current row) duration_sec_count_90d
    from web_events
    where event_time >= '2025-06-01'
)
select
    s.id,
    s.session_id,
    s.event_type,
    s.page,
    s.event_time,
    s.duration_sec,
    s.last_known_duration_sec,
    s.duration_sec_count_90d,
    case when event_type rlike '^(click|search)$' then 'PRIORITY' when event_type rlike '^(page_view|add_to_cart)$' then 'OTHER_ZONE' else 'EXTENDED' end as event_type_zone,
    (select sum(x.duration_sec) from web_events x where x.session_id = s.session_id) as sum_web_event_duration_sec
from metrics s
order by event_time desc, event_type_zone;
