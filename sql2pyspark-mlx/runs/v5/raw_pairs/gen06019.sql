with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        k,
        v
    from app_logs
    lateral view explode(context) li as k, v
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when log_ts - lag(log_ts) over (partition by service order by log_ts, id, k) > interval '15' minute then 1 else 0 end as is_new_session
    from flattened
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by service order by log_ts, id, k rows between unbounded preceding and current row) as session_no
    from with_gaps
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        lead(session_no, 1) over (partition by k, service order by log_ts, id, k) as next_session_no,
        last_value(is_new_session) ignore nulls over (partition by k order by is_new_session, id, k rows between unbounded preceding and current row) as last_known_is_new_session,
        row_number() over (partition by k order by log_ts desc, id, k) as event_seq
    from sessionized
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        sum(last_known_is_new_session) over w_hist as running_last_known_is_new_session,
        avg(session_no) over (partition by host, k order by session_no, id, k rows between 3 preceding and current row) as session_no_avg_3,
        avg(event_seq) over (order by cast(log_ts as bigint) range between 900 preceding and current row) as event_seq_avg_15m
    from scored
    where session_no > 7
    window w_hist as (partition by service order by session_no, id, k rows between unbounded preceding and current row)
),
grouped as (
    -- Multi-level aggregation
    select
        case when event_seq >= 8 then 'PREMIUM' else 'STANDARD' end as event_seq_tier,
        case when session_no_avg_3 >= 7 then 'PREMIUM' else 'STANDARD' end as session_no_avg_3_tier,
        max(log_ts) as last_seen,
        approx_percentile(is_new_session, array(0.25, 0.5, 0.75)) as is_new_session_quartiles,
        max(log_ts) as last_seen_2,
        max(log_ts) as ended_at,
        grouping_id() as grouping_level
    from enriched
    group by cube (case when event_seq >= 8 then 'PREMIUM' else 'STANDARD' end, case when session_no_avg_3 >= 7 then 'PREMIUM' else 'STANDARD' end)
)
select
    session_no_avg_3_tier,
    last_seen,
    is_new_session_quartiles,
    grouping_level
from grouped
order by grouping_level desc, session_no_avg_3_tier;
