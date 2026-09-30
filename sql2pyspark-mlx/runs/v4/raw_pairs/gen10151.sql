with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    from app_logs
    lateral view explode(context) t as attr_key, attr_value
)
-- Window metrics over several different partitions
select
    id,
    host,
    latency_ms,
    status_code,
    attr_key,
    attr_value,
    lag(latency_ms, 1) over (partition by host order by latency_ms, id, attr_key) as prev_latency_ms,
    last_value(attr_key) ignore nulls over (partition by host order by status_code, id, attr_key rows between unbounded preceding and current row) as last_known_attr_key
from flattened
