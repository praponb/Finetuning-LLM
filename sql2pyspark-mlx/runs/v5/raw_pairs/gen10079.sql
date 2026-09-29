with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    from app_logs
    lateral view outer explode(context) t as attr_key, attr_value
),
parsed as (
    -- Higher-order array functions
    select
        id,
        log_ts,
        level,
        status_code,
        attr_key,
        attr_value,
        cast((status_code & 8) != 0 as int) as has_fault,
        shiftright(status_code, 2) as status_code_high
    from lines
),
matched_keys as (
    -- Set difference on keys
    select id, level
    from parsed
    except all
    select id, level
    from app_logs
    where status_code != 374
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from parsed s
    inner join matched_keys k on s.id = k.id and s.level = k.level
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        quarter(log_ts) as log_ts_quarter,
        date_trunc('week', log_ts) as log_ts_period
    from verified
    where attr_key is not null
),
remaining_keys as (
    -- Set difference on keys
    select id, level
    from filtered
    except all
    select id, level
    from app_logs
    where level = 'INFO'
),
verified_app_logs as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join remaining_keys k on s.id = k.id and s.level = k.level
)
select
    log_ts,
    level,
    status_code,
    attr_key,
    has_fault,
    status_code_high,
    log_ts_period
from verified_app_logs;
