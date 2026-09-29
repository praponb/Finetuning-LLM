with latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by event_type order by event_time desc, id asc) as row_num
    from web_events
    qualify row_num = 1
)
-- Aggregate with conditional metrics
select
    case when duration_sec >= 475 then 'PREMIUM' else 'STANDARD' end as duration_sec_tier,
    avg(duration_sec) as avg_duration_sec,
    size(collect_set(event_type)) as num_event_types
from latest
group by 1
order by num_event_types asc
