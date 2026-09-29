with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    from app_logs
    lateral view outer explode(context) li as k, value
)
-- Grouped metrics
select
    level,
    host,
    min(log_ts) as first_log_ts,
    max(latency_ms) filter (where service = 'billing') as billing_max_latency_ms,
    percent_rank() over (partition by level order by max(latency_ms) filter (where service = 'billing') desc) as billing_max_latency_ms_percent_rank
from flattened
group by level, host
having billing_max_latency_ms > 0
order by host desc nulls last;
