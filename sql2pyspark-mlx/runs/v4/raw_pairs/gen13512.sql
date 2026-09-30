with flattened as (
    select
        id,
        service,
        host,
        log_ts,
        status_code,
        k,
        v
    from app_logs
    lateral view explode(context) x as k, v
),
prev_events as (
    select *, lag(log_ts) over (partition by service order by log_ts asc, id asc, k asc) as prev_log_ts
    from flattened
),
flagged as (
    select *, case when prev_log_ts is null or log_ts > prev_log_ts + interval '20' minute then 1 else 0 end as session_start_flag
    from prev_events
),
sessions_app_logs as (
    select *, sum(session_start_flag) over (partition by service order by log_ts asc, id asc, k asc rows between unbounded preceding and current row) as session_id
    from flagged
),
metrics as (
    select
        *,
        sum(status_code) over w_recent running_status_code,
        lead(session_start_flag, 1) over (partition by host order by session_start_flag asc, id asc, k asc) next_session_start_flag,
        ntile(5) over (partition by k order by session_start_flag asc, id asc, k asc) session_start_flag_bucket
    from sessions_app_logs
    window w_recent as (partition by host order by session_id asc, id asc, k asc rows between unbounded preceding and current row)
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by service order by running_status_code asc nulls first, id asc, k asc) as latest_rank
        from metrics
    ) x
    where latest_rank = 1
),
with_gaps as (
    select *, case when lag(log_ts) over (partition by host order by log_ts asc, id asc, k asc) is null
                 or log_ts > lag(log_ts) over (partition by host order by log_ts asc, id asc, k asc) + interval '15' minute then 1 else 0 end as session_start_flag_2
    from deduped
),
sessions_app_logs_2 as (
    select *, sum(session_start_flag_2) over (partition by host order by log_ts asc, id asc, k asc rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
flagged_2 as (
    select *, case when lag(log_ts) over (partition by k order by log_ts asc, id asc, k asc) is null
                 or log_ts > lag(log_ts) over (partition by k order by log_ts asc, id asc, k asc) + interval '30' minute then 1 else 0 end as session_start_flag_3
    from sessions_app_logs_2
),
islands as (
    select *, sum(session_start_flag_3) over (partition by k order by log_ts asc, id asc, k asc rows between unbounded preceding and current row) as island_id
    from flagged_2
)
select
    id,
    service,
    host,
    log_ts,
    status_code,
    k,
    v,
    prev_log_ts,
    session_start_flag,
    session_id,
    running_status_code,
    next_session_start_flag,
    session_start_flag_bucket,
    latest_rank,
    session_start_flag_2,
    burst_id,
    session_start_flag_3,
    island_id,
    rank() over (partition by service order by session_start_flag_bucket asc nulls last) session_start_flag_bucket_rank,
    first_value(running_status_code, true) over (partition by k order by log_ts asc, id asc, k asc rows between unbounded preceding and current row) first_known_running_status_code
from islands;
