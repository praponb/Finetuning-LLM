with flattened as (
    select
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    from app_logs
    lateral view explode(context) e as attr_key, v
),
with_prev as (
    select *, lag(log_ts) over (partition by level order by log_ts, id, attr_key) as prev_log_ts
    from flattened
),
with_gaps as (
    select *, case when prev_log_ts is null or log_ts > prev_log_ts + interval '45' minute then 1 else 0 end as is_new_session
    from with_prev
),
sessionized as (
    select *, sum(is_new_session) over (partition by level order by log_ts, id, attr_key rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
base as (
    select
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        prev_log_ts,
        is_new_session,
        round(is_new_session * 100, 2) as is_new_session_scaled,
        coalesce(attr_key, 'UNKNOWN') as attr_key_clean
    from sessionized
    where log_ts > date '2025-06-01'
      and attr_key is not null
      and attr_key in ('region', 'version')
),
remaining_keys as (
    select host, id
    from base
    except
    select host, id
    from app_logs
    where latency_ms < 1486
),
kept_rows as (
    select s.*
    from base s
    inner join remaining_keys k on s.host = k.host and s.id = k.id
)
select
    id,
    host,
    log_ts,
    level,
    attr_key,
    prev_log_ts,
    is_new_session_scaled
from kept_rows
