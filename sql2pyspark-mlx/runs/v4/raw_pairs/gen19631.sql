with with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by device order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by device order by started_at, id) + interval '7' day then 1 else 0 end as session_start_flag
    from sessions
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by device order by started_at, id rows between unbounded preceding and current row) as session_no
    from with_gaps
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when started_at - lag(started_at) over (partition by customer_id order by started_at, id) > interval 30 days then 1 else 0 end as session_start_flag_2
    from sessionized
),
sessions_sessions as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag_2) over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as session_no_2
    from gaps
),
metrics as (
    -- Ranking and gap features
    select
        *,
        lag(session_no_2, 1) over (order by session_no_2, id) as prev_session_no_2,
        sum(session_no_2) over (partition by device order by session_start_flag_2, id rows between unbounded preceding and current row) as running_session_no_2,
        sum(session_start_flag) over w_grp as running_session_start_flag,
        avg(session_start_flag_2) over w_recent as session_start_flag_2_avg_7d,
        ntile(4) over (partition by customer_id order by session_no_2, id) as session_no_2_bucket
    from sessions_sessions
    window w_grp as (partition by device order by session_start_flag, id rows between unbounded preceding and current row), w_recent as (partition by browser, device order by cast(started_at as bigint) range between 604800 preceding and current row)
),
gaps_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when started_at - lag(started_at) over (partition by customer_id order by started_at, id) > interval 14 days then 1 else 0 end as is_new_session
    from metrics
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as session_id
    from gaps_2
)
select
    id,
    device,
    browser,
    started_at,
    country,
    session_start_flag,
    session_no,
    prev_session_no_2,
    running_session_no_2,
    case when device rlike '^(mobile|desktop)$' then 'PRIORITY' else 'EXTENDED' end as device_zone
from islands
order by country;
