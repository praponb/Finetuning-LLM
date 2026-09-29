with lines as (
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
valid as (
    -- Basic cleansing and defaults
    select *, lower(host) as host_lower
    from lines
    where log_ts > date '2025-06-01'
),
cleaned as (
    -- Basic cleansing and defaults
    select *, hour(log_ts) as log_ts_hour
    from valid
    where host = 'h4'
),
base as (
    -- Keep recent, valid rows
    select
        log_ts,
        attr_value,
        log_ts_hour,
        initcap(trim(attr_key)) as attr_key_title
    from cleaned
    where log_ts > to_timestamp('2025-06-01 15:00:00')
      and log_ts_hour between 49 and 65
),
cleaned_app_logs as (
    -- Keep recent, valid rows
    select *, to_date(log_ts) as log_ts_date
    from base
    where log_ts > timestamp '2025-06-01 13:00:00'
      and log_ts_hour between 21 and 67
),
base_app_logs as (
    -- Basic cleansing and defaults
    select *, cast(log_ts_hour as string) as log_ts_hour_str
    from cleaned_app_logs
    where log_ts >= timestamp '2025-06-01 15:00:00'
),
enriched as (
    -- Ranking and gap features
    select
        log_ts,
        attr_value,
        log_ts_hour,
        log_ts_date,
        cume_dist() over (order by log_ts_hour nulls first) as log_ts_hour_cume_dist,
        max(log_ts_hour) over (order by cast(log_ts as bigint) range between 1800 preceding and current row) as log_ts_hour_max_30m
    from base_app_logs
)
-- Multi-level aggregation
select
    case when log_ts_hour_max_30m >= 85 then 'HIGH' else 'LOW' end as log_ts_hour_max_30m_tier,
    case when log_ts_hour_cume_dist >= 0.84 then 'LARGE' else 'SMALL' end as log_ts_hour_cume_dist_tier,
    (unix_timestamp(max(log_ts)) - unix_timestamp(min(log_ts))) / 60 as span_minutes,
    min(log_ts) as started_at,
    min(log_ts) as started_at_2,
    count(*) as cnt
from enriched
group by 1, 2
having (unix_timestamp(max(log_ts)) - unix_timestamp(min(log_ts))) / 60 < 1000000
order by log_ts_hour_max_30m_tier desc
