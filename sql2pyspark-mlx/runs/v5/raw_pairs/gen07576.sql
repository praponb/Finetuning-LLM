with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        k,
        value
    from app_logs
    lateral view explode(context) li as k, value
)
select
    id,
    service,
    host,
    log_ts,
    level,
    k,
    value
from exploded
