with enriched as (
    select
        *,
        lag(duration_sec, 1) over (partition by page order by duration_sec, id) as prev_duration_sec,
        ntile(5) over (partition by event_type order by duration_sec, id) as duration_sec_bucket
    from web_events
)
select
    case when duration_sec >= 395 then 'PREMIUM' else 'STANDARD' end as duration_sec_tier,
    page,
    bit_or(duration_sec) as duration_sec_bit_or,
    max(duration_sec_bucket) as max_duration_sec_bucket,
    sum(duration_sec) filter (where page = '/checkout') as checkout_total_duration_sec,
    round(avg(duration_sec_bucket), 2) as avg_duration_sec_bucket,
    count(case when event_type = 'search' then 1 end) as search_count,
    avg(duration_sec_bucket) as avg_duration_sec_bucket_2,
    dense_rank() over (partition by case when duration_sec >= 395 then 'PREMIUM' else 'STANDARD' end order by round(avg(duration_sec_bucket), 2) desc) as avg_duration_sec_bucket_dense_rank
from enriched
group by duration_sec_tier, page
order by checkout_total_duration_sec desc nulls last
