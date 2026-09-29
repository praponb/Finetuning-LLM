with lines as (
    select
        id,
        service,
        log_ts,
        latency_ms,
        attr_key,
        attr_value
    from app_logs
    lateral view explode(context) li as attr_key, attr_value
),
prev_events as (
    select *, lag(log_ts) over (partition by attr_key order by log_ts, id, attr_key) as prev_log_ts
    from lines
),
gaps as (
    select *, case when prev_log_ts is null or log_ts > prev_log_ts + interval 15 minutes then 1 else 0 end as is_new_session
    from prev_events
),
islands as (
    select *, sum(is_new_session) over (partition by attr_key order by log_ts, id, attr_key rows between unbounded preceding and current row) as session_no
    from gaps
),
deduped as (
    select *
    from islands
    qualify row_number() over (partition by service order by log_ts, id, attr_key) = 1
),
latest as (
    select *
    from (
        select *, row_number() over (partition by attr_key order by session_no desc, id, attr_key) as row_num
        from deduped
    ) t
    where row_num = 1
),
gaps_2 as (
    select *, case when prev_log_ts - lag(prev_log_ts) over (partition by service order by prev_log_ts, id, attr_key) > interval '7' day then 1 else 0 end as is_new_burst
    from latest
),
islands_app_logs as (
    select *, sum(is_new_burst) over (partition by service order by prev_log_ts, id, attr_key rows between unbounded preceding and current row) as island_id
    from gaps_2
)
select
    id,
    service,
    attr_key,
    attr_value,
    prev_log_ts,
    is_new_session,
    session_no,
    row_num,
    island_id
from islands_app_logs
order by is_new_session desc nulls last;
