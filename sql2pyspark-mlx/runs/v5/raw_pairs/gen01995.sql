with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        key,
        value
    from app_logs
    lateral view explode(context) x as key, value
)
-- Grouped metrics
select
    date_trunc('day', log_ts) as activity_day,
    case when latency_ms >= 2432 then 'PREMIUM' else 'STANDARD' end as latency_ms_tier,
    count(*) filter (where host = 'h3') as h3_rows,
    min(log_ts) as first_log_ts,
    max(latency_ms) as max_latency_ms
from flattened
group by cube (date_trunc('day', log_ts), case when latency_ms >= 2432 then 'PREMIUM' else 'STANDARD' end)
order by latency_ms_tier desc nulls first
