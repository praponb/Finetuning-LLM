with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        k,
        value
    from app_logs
    lateral view explode(context) x as k, value
)
select
    id,
    service,
    host,
    level,
    latency_ms,
    status_code,
    k
from flattened
order by service desc nulls first;
