with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        level,
        attr_key,
        attr_value
    from app_logs
    lateral view outer explode(context) e as attr_key, attr_value
)
select
    id,
    host,
    log_ts,
    level,
    attr_key
from exploded
order by log_ts;
