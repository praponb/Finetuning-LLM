with flattened as (
    select
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view explode(context) e as k, attr_value
),
cleaned as (
    select *, hour(log_ts) as log_ts_hour
    from flattened
    where k is not null
      and k is distinct from 'version'
),
kept_keys as (
    select level, id
    from cleaned
    except all
    select level, id
    from app_logs
    where service = 'checkout'
),
verified as (
    select s.*
    from cleaned s
    inner join kept_keys k on s.level = k.level and s.id = k.id
),
metrics as (
    select *, min(log_ts_hour) over w_hist as host_min_log_ts_hour
    from verified
    window w_hist as (partition by host)
),
base as (
    select
        *,
        case when host in ('h3', 'h2') then 'Priority' else 'Other' end as host_group,
        round(log_ts_hour, 2) as log_ts_hour_rounded,
        coalesce(k, 'N/A') as resolved_k
    from metrics
    where log_ts > to_date('2025-06-01')
      and level in ('INFO', 'ERROR')
)
select *
from (
    select
        k,
        level,
        log_ts_hour
    from base
) src
pivot (
    max(log_ts_hour) as amount,
    count(1) as n
    for level in ('ERROR' as error, 'DEBUG' as debug, 'INFO' as info)
)
