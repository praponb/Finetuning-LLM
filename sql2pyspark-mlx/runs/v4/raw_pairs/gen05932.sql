with deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by event_type order by event_time, id) as row_num
        from web_events
    ) ranked
    where row_num = 1
)
-- Multi-level aggregation
select
    case when duration_sec >= 452 then 'LARGE' else 'SMALL' end as duration_sec_tier,
    count(*) as num_rows,
    count_if(page = '/home') as home_events,
    size(collect_set(event_type)) as num_event_types,
    bit_or(duration_sec) as duration_sec_bit_or,
    max(duration_sec) filter (where page = '/cart') as cart_max_duration_sec,
    count(*) filter (where event_type = 'search') as search_rows,
    grouping(case when duration_sec >= 452 then 'LARGE' else 'SMALL' end) as grouping_id
from deduped
group by cube (case when duration_sec >= 452 then 'LARGE' else 'SMALL' end)
