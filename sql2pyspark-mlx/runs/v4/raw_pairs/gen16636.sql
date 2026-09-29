with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        k,
        v
    from app_logs
    lateral view outer explode(context) t as k, v
),
valid as (
    -- Keep recent, valid rows
    select *, length(service) as service_length
    from flattened
    where k is not null
      and service = 'search'
),
base as (
    -- Keep recent, valid rows
    select *, case when service in ('search', 'auth') then 'Core' else 'Other' end as service_group
    from valid
    where k = 'trace'
),
remaining_keys as (
    -- Set difference on keys
    select id, service
    from base
    except distinct
    select id, service
    from app_logs
    where log_ts >= '2024-05-01'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    join remaining_keys k on s.id = k.id and s.service = k.service
)
-- Window metrics over several different partitions
select
    id,
    service,
    log_ts,
    k,
    v,
    service_length,
    service_group,
    max(service_length) over (partition by k, service order by cast(log_ts as bigint) range between 900 preceding and current row) as service_length_max_15m,
    count(service_length) over (partition by service order by cast(log_ts as bigint) range between 1800 preceding and current row) as service_length_count_30m
from verified;
