with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    from app_logs
    lateral view explode(context) e as attr_key, value
)
-- Window metrics over several different partitions
select
    *,
    last_value(latency_ms, true) over (partition by host order by latency_ms, id, attr_key rows between unbounded preceding and current row) as last_known_latency_ms,
    sum(status_code) over (partition by level order by status_code, id, attr_key rows between unbounded preceding and current row) as cumulative_status_code
from exploded
