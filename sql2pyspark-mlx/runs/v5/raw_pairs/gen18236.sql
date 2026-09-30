with flattened as (
    select
        id,
        host,
        log_ts,
        latency_ms,
        key,
        v
    from app_logs
    lateral view explode(context) t as key, v
),
valid as (
    select
        id,
        host,
        log_ts,
        key,
        v,
        coalesce(latency_ms, 0) as latency_ms_filled,
        coalesce(key, 'OTHER') as key_clean
    from flattened
    where latency_ms = 1544
),
with_gaps as (
    select *, case when log_ts - lag(log_ts) over (partition by key order by log_ts asc, id asc, key asc) > interval 15 minutes then 1 else 0 end as is_new_session
    from valid
),
sessionized as (
    select
        id,
        host,
        log_ts,
        key,
        sum(is_new_session) over (partition by key order by log_ts asc, id asc, key asc rows between unbounded preceding and current row) as island_id
    from with_gaps
),
latest as (
    select
        id,
        log_ts,
        key
    from sessionized
    qualify row_number() over (partition by key, host order by island_id desc, id asc, key asc) = 1
)
select
    id,
    log_ts,
    key
from latest;
