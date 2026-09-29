with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        level,
        status_code,
        k,
        value
    from app_logs
    lateral view outer explode(context) x as k, value
),
flattened_attrs as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        log_ts,
        level,
        k,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 8) != 0 as int) as has_fault
    from lines
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        log_ts,
        k,
        status_code_high,
        has_fault,
        round(status_code_high, 0) as status_code_high_rounded
    from flattened_attrs
    where log_ts >= date '2025-06-01'
      and k is not null
),
cleaned_app_logs as (
    -- Basic cleansing and defaults
    select
        id,
        log_ts,
        status_code_high,
        has_fault,
        initcap(trim(k)) as k_title
    from cleaned
    where log_ts >= date '2025-06-01'
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        log_ts,
        status_code_high,
        has_fault,
        k_title,
        datediff(current_date(), log_ts) as days_since_log_ts
    from cleaned_app_logs
    where log_ts >= to_timestamp('2025-06-01 10:00:00')
      and to_date(log_ts) < '2024-11-01'
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        log_ts,
        status_code_high,
        has_fault,
        k_title,
        days_since_log_ts,
        days_since_log_ts / nullif(status_code_high, 0) as days_since_log_ts_per_status_code_high,
        greatest(status_code_high, days_since_log_ts) as greatest_status_code_high_days_since_log_ts
    from base
    where status_code_high <> 1
),
stats as (
    -- Grouped metrics
    select
        date_trunc('hour', log_ts) as hour_window,
        max(log_ts) as ended_at,
        count(distinct id) as distinct_ids
    from filtered
    group by hour_window
    having distinct_ids > 1
)
select
    hour_window,
    ended_at,
    distinct_ids
from stats
order by hour_window desc nulls last
