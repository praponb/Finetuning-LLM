with flattened as (
    select
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        value
    from app_logs
    lateral view explode(context) x as k, value
),
deduped as (
    select *
    from (
        select *, rank() over (partition by host, k order by log_ts) as row_num
        from flattened
    ) t
    where row_num = 1
),
flagged as (
    select *, case when log_ts - lag(log_ts) over (partition by host order by log_ts, id, k) > interval '20' minute then 1 else 0 end as new_group_flag
    from deduped
),
sessions_app_logs as (
    select *, sum(new_group_flag) over (partition by host order by log_ts, id, k rows between unbounded preceding and current row) as session_id
    from flagged
),
metrics as (
    select
        *,
        lag(status_code) over w_running as prev_status_code,
        lag(session_id) over w_running as prev_session_id,
        lead(session_id) over w_running as next_session_id,
        row_number() over w_running as seq_no,
        sum(latency_ms) over (partition by k order by new_group_flag, id, k rows between unbounded preceding and current row) as running_latency_ms
    from sessions_app_logs
    window w_running as (partition by host order by status_code, id, k)
),
totals as (
    select
        date_trunc('hour', log_ts) as hour_window,
        case when next_session_id >= 6 then 'HIGH' else 'LOW' end as next_session_id_tier,
        count_if(k = 'trace') as trace_events,
        max(latency_ms) filter (where k = 'trace') as trace_max_latency_ms,
        count(*) as row_count
    from metrics
    group by date_trunc('hour', log_ts), case when next_session_id >= 6 then 'HIGH' else 'LOW' end
    having trace_max_latency_ms < 1000000
)
select
    hour_window,
    next_session_id_tier,
    trace_events,
    row_count
from totals
order by next_session_id_tier desc nulls first, trace_events desc;
