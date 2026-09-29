with filtered as (
    -- Keep recent, valid rows
    select *, cast(duration_sec as double) as duration_sec_str
    from web_events
    where page is not null
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(event_time) over (partition by page order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by page order by event_time, id) + interval 7 days then 1 else 0 end as session_start_flag
    from filtered
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by page order by event_time, id rows between unbounded preceding and current row) as island_id
    from gaps
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when event_time - lag(event_time) over (partition by page order by event_time, id) > interval '14' day then 1 else 0 end as is_new_burst
    from islands
),
sessions_web_events as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by page order by event_time, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        burst_id + duration_sec as burst_id_plus_duration_sec,
        island_id - is_new_burst as island_id_minus_is_new_burst
    from sessions_web_events
    where page is not null
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        duration_sec_str,
        session_start_flag,
        island_id,
        is_new_burst,
        burst_id,
        burst_id_plus_duration_sec,
        island_id_minus_is_new_burst,
        ntile(5) over (partition by event_type order by duration_sec, id) as duration_sec_bucket,
        lag(burst_id, 2) over (partition by page, session_id order by duration_sec, id) as prev_burst_id,
        sum(is_new_burst) over (partition by event_type order by burst_id_plus_duration_sec, id rows between unbounded preceding and current row) as running_is_new_burst
    from cleaned
),
with_gaps_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when event_time - lag(event_time) over (partition by page order by event_time, id) > interval 90 days then 1 else 0 end as is_new_burst_2
    from with_windows
),
sessions_web_events_2 as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst_2) over (partition by page order by event_time, id rows between unbounded preceding and current row) as session_id_2
    from with_gaps_2
)
select
    s.id,
    s.session_id,
    s.event_type,
    s.event_time,
    s.duration_sec_str,
    s.island_id,
    s.is_new_burst,
    s.burst_id_plus_duration_sec,
    s.island_id_minus_is_new_burst,
    (select sum(w2.duration_sec) from web_events w2 where w2.session_id = s.session_id) as sum_web_event_duration_sec
from sessions_web_events_2 s
order by session_id
