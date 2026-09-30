with deduped as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by page, event_type order by event_time desc nulls last, id) as latest_rank
        from web_events
    ) ranked
    where latest_rank = 1
)
-- Multi-level aggregation
select
    event_type,
    date_trunc('quarter', event_time) as quarter_start,
    case when duration_sec >= 500 then 'HIGH' else 'LOW' end as duration_sec_tier,
    count_if(page = '/cart') as cart_events,
    percentile_approx(duration_sec, 0.5) as duration_sec_p50,
    count(*) as event_count
from deduped
group by event_type, date_trunc('quarter', event_time), case when duration_sec >= 500 then 'HIGH' else 'LOW' end
