with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        k,
        value
    from app_logs
    lateral view outer explode(context) x as k, value
),
cleaned as (
    -- Basic cleansing and defaults
    select
        log_ts,
        k,
        value,
        date_format(log_ts, 'yyyy-MM-dd') as log_ts_label,
        coalesce(k, 'NONE') as k_clean
    from flattened
    where log_ts > date '2025-06-01'
      and k is not null
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        log_ts,
        k,
        value,
        log_ts_label,
        k_clean,
        case when k in ('trace', 'region') then 'Core' else 'Other' end as k_group
    from cleaned
    where log_ts > date '2025-06-01'
      and k is not null
      and hour(log_ts) between 12 and 18
),
filtered_app_logs as (
    -- Keep recent, valid rows
    select *, length(k) as k_length
    from filtered
    where log_ts >= timestamp '2025-06-02 01:00:00'
),
filtered_app_logs_2 as (
    -- Keep recent, valid rows
    select *, hour(log_ts) as log_ts_hour
    from filtered_app_logs
    where log_ts > date '2025-06-01'
)
select
    log_ts,
    value,
    log_ts_label,
    k_clean,
    k_group,
    k_length
from filtered_app_logs_2
order by log_ts, k_length desc;
