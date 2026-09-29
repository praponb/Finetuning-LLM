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
        key,
        value
    from app_logs
    lateral view outer explode(context) x as key, value
),
unpacked as (
    -- Struct fields, map lookups and JSON extraction
    select
        service,
        level,
        latency_ms,
        value,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 4) != 0 as int) as has_fault
    from flattened
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        service,
        latency_ms,
        value,
        status_code_high,
        has_fault,
        greatest(status_code_high, has_fault) greatest_status_code_high_has_fault
    from unpacked
    where has_fault = 0
),
valid_app_logs as (
    -- Keep recent, valid rows
    select *, latency_ms + status_code_high latency_ms_plus_status_code_high
    from valid
    where status_code_high = 2
)
select
    service,
    value,
    latency_ms_plus_status_code_high
from valid_app_logs
order by latency_ms_plus_status_code_high desc;
