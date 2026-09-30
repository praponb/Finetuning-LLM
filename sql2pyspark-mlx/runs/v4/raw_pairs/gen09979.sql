with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    from app_logs
    lateral view explode(context) t as k, value
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        first_value(k, true) over (partition by k order by log_ts, id, k rows between unbounded preceding and current row) first_known_k,
        round(latency_ms / sum(latency_ms) over (partition by service), 4) latency_ms_share,
        corr(status_code, latency_ms) over (partition by host) corr_status_code_latency_ms
    from lines
),
prev_events as (
    -- Previous event time per key
    select *, lag(log_ts) over (partition by host order by log_ts, id, k) as prev_log_ts
    from enriched
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_log_ts is null or log_ts > prev_log_ts + interval 30 minutes then 1 else 0 end as session_start_flag
    from prev_events
),
sessions_app_logs as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by host order by log_ts, id, k rows between unbounded preceding and current row) as session_no
    from with_gaps
),
enriched_app_logs as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by service order by prev_log_ts, id, k) row_seq,
        lead(session_start_flag, 1, 0) over (partition by k order by prev_log_ts, id, k) next_session_start_flag
    from sessions_app_logs
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when prev_log_ts - lag(prev_log_ts) over (partition by k order by prev_log_ts, id, k) > interval '7' day then 1 else 0 end as is_new_session
    from enriched_app_logs
),
sessions_app_logs_2 as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by k order by prev_log_ts, id, k rows between unbounded preceding and current row) as island_id
    from flagged
)
-- Multi-level aggregation
select
    case when next_session_start_flag >= 1 then 'HIGH' else 'LOW' end as next_session_start_flag_tier,
    (unix_timestamp(max(log_ts)) - unix_timestamp(min(log_ts))) / 60 as span_minutes,
    count(*) filter (where level = 'DEBUG') as debug_rows,
    sum(case when host = 'h3' then latency_ms_share else 0 end) as h3_latency_ms_share,
    approx_percentile(next_session_start_flag, array(0.25, 0.5, 0.75)) as next_session_start_flag_quartiles,
    sum(session_no) filter (where level = 'INFO') as info_total_session_no
from sessions_app_logs_2
group by case when next_session_start_flag >= 1 then 'HIGH' else 'LOW' end;
