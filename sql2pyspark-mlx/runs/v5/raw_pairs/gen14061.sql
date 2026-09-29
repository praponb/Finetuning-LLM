with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        key,
        v
    from app_logs
    lateral view outer explode(context) t as key, v
),
valid as (
    -- Keep recent, valid rows
    select
        service,
        host,
        log_ts,
        key,
        hour(log_ts) as log_ts_hour
    from exploded
    where key is not null
      and key is distinct from 'region'
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        service,
        host,
        key,
        case when host in ('h1', 'h4') then 'Core' else 'Other' end as host_group,
        coalesce(log_ts_hour, 0) as log_ts_hour_filled
    from valid
    where key is not null
      and key is distinct from 'trace'
),
cleaned_app_logs as (
    -- Basic cleansing and defaults
    select
        service,
        host_group,
        trim(host) as clean_host
    from cleaned
    where key is distinct from 'region'
),
valid_app_logs as (
    -- Filter the raw rows and derive helper columns
    select *, trim(service) as clean_service
    from cleaned_app_logs
    where service = 'auth'
),
grouped as (
    -- Multi-level aggregation
    select
        service,
        count(case when service = 'billing' then 1 end) as billing_count,
        count(*) filter (where service = 'auth') as auth_rows,
        count(distinct service) as distinct_services
    from valid_app_logs
    group by service
)
select
    service,
    billing_count,
    auth_rows
from grouped
order by billing_count
