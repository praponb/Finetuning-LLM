with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    from app_logs
    lateral view explode(context) t as attr_key, value
)
select
    id,
    status_code,
    attr_key,
    value
from exploded
