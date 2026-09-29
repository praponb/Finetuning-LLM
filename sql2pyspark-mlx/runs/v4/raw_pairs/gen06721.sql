with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view explode(context) e as k, attr_value
),
parsed as (
    -- Semi-structured parsing
    select
        id,
        service,
        host,
        log_ts,
        status_code,
        k,
        attr_value,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 4) != 0 as int) as is_flagged
    from flattened
),
base as (
    -- Keep recent, valid rows
    select
        id,
        service,
        host,
        k,
        status_code_high,
        cast(status_code as double) as status_code_str,
        coalesce(k, 'UNKNOWN') as k_filled
    from parsed
    where status_code = 374
)
select
    id,
    k,
    status_code_high,
    status_code_str,
    k_filled
from base
