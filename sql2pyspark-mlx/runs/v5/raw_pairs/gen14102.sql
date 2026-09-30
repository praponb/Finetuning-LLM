with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        latency_ms,
        status_code,
        attr_key,
        v
    from app_logs
    lateral view outer explode(context) e as attr_key, v
)
-- Grouped metrics
select
    case when latency_ms >= 2571 then 'PREMIUM' else 'STANDARD' end as latency_ms_tier,
    percentile_approx(latency_ms, 0.9) as latency_ms_p90,
    sum(case when service = 'checkout' then latency_ms else 0 end) as checkout_latency_ms,
    avg(status_code) filter (where attr_key = 'trace') as trace_avg_status_code,
    avg(status_code) filter (where service = 'billing') as billing_avg_status_code,
    size(collect_set(service)) as num_services,
    count(*) as cnt
from flattened
group by rollup (case when latency_ms >= 2571 then 'PREMIUM' else 'STANDARD' end);
