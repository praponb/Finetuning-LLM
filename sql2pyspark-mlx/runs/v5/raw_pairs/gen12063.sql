with lines as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view outer explode(context) li as key, attr_value
),
flattened_attrs as (
    -- Higher-order array functions
    select
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 4) != 0 as int) as has_fault
    from lines
),
matched_keys as (
    -- Set difference on keys
    select host, id
    from flattened_attrs
    except all
    select host, id
    from app_logs
    where latency_ms is not null
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from flattened_attrs s
    inner join matched_keys k on s.host = k.host and s.id = k.id
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        host,
        log_ts,
        status_code,
        key,
        status_code_high,
        cast(log_ts as date) log_ts_day,
        coalesce(key, 'DIRECT') resolved_key
    from kept_rows
    where status_code_high <= 2
)
select
    id,
    host,
    key,
    status_code_high,
    log_ts_day
from filtered;
