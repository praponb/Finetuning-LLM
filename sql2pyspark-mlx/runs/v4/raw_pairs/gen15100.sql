with flattened as (
    select
        id,
        service,
        host,
        latency_ms,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) x as k, attr_value
),
enriched as (
    select
        id,
        service,
        host,
        latency_ms,
        k,
        attr_value,
        dense_rank() over (order by latency_ms desc nulls last) as latency_ms_dense_rank,
        percent_rank() over (order by latency_ms desc nulls last) as latency_ms_percent_rank
    from flattened
),
filtered as (
    select
        id,
        service,
        host,
        latency_ms,
        k,
        latency_ms_dense_rank,
        latency_ms_percent_rank,
        greatest(latency_ms_percent_rank, latency_ms) as greatest_latency_ms_percent_rank_latency_ms,
        lower(service) as service_lower
    from enriched
    where k is not null
),
metrics as (
    select
        id,
        service,
        host,
        latency_ms,
        k,
        latency_ms_dense_rank,
        latency_ms_percent_rank,
        greatest_latency_ms_percent_rank_latency_ms,
        service_lower,
        rank() over (partition by host order by latency_ms_percent_rank) as latency_ms_percent_rank_rank,
        rank() over (partition by k order by greatest_latency_ms_percent_rank_latency_ms) as greatest_latency_ms_percent_rank_latency_ms_rank
    from filtered
),
summary as (
    select
        case when latency_ms_dense_rank >= 8 then 'LARGE' else 'SMALL' end as latency_ms_dense_rank_tier,
        approx_percentile(greatest_latency_ms_percent_rank_latency_ms_rank, 0.95) as greatest_latency_ms_percent_rank_latency_ms_rank_p95,
        percentile_approx(latency_ms_percent_rank_rank, 0.5) as latency_ms_percent_rank_rank_p50,
        avg(greatest_latency_ms_percent_rank_latency_ms) filter (where service = 'checkout') as checkout_avg_greatest_latency_ms_percent_rank_latency_ms
    from metrics
    group by cube (case when latency_ms_dense_rank >= 8 then 'LARGE' else 'SMALL' end)
)
select
    latency_ms_dense_rank_tier,
    greatest_latency_ms_percent_rank_latency_ms_rank_p95,
    latency_ms_percent_rank_rank_p50
from summary
order by latency_ms_dense_rank_tier;
