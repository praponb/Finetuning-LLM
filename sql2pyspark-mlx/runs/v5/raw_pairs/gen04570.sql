with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        status_code,
        attr_key,
        attr_value
    from app_logs
    lateral view outer explode(context) e as attr_key, attr_value
),
parsed as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        host,
        log_ts,
        cast((status_code & 2) != 0 as int) as has_fault,
        shiftright(status_code, 2) as status_code_high
    from exploded
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        has_fault,
        status_code_high,
        count(has_fault) over (order by cast(log_ts as bigint) range between 3600 preceding and current row) as has_fault_count_1h,
        percent_rank() over (partition by host order by status_code_high desc) as status_code_high_percent_rank,
        max(status_code_high) over (partition by host order by cast(log_ts as bigint) range between 3600 preceding and current row) as status_code_high_max_1h,
        rank() over (partition by host order by status_code_high desc nulls last) as status_code_high_rank
    from parsed
    where has_fault = 1
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        status_code_high + has_fault as status_code_high_plus_has_fault,
        coalesce(status_code_high, 0) as status_code_high_filled
    from metrics
    where status_code_high_rank >= 5
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        has_fault,
        status_code_high_rank,
        status_code_high_filled,
        round(status_code_high_filled, 0) as status_code_high_filled_rounded
    from cleaned
    where status_code_high >= 1
),
cleaned_app_logs as (
    -- Basic cleansing and defaults
    select
        id,
        has_fault,
        status_code_high_rank,
        status_code_high_filled,
        case when status_code_high_filled >= 85 then 'High' when status_code_high_filled >= 18 then 'Medium' else 'Low' end as status_code_high_filled_level
    from valid
    where status_code_high_filled_rounded > 40
),
base as (
    -- Keep recent, valid rows
    select
        id,
        status_code_high_rank,
        status_code_high_filled,
        least(status_code_high_filled, status_code_high_rank) as least_status_code_high_filled_status_code_high_rank
    from cleaned_app_logs
    where has_fault = 0
)
select
    id,
    status_code_high_rank,
    status_code_high_filled
from base
