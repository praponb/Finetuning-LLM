with ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by page order by event_time nulls first, id) as latest_rank
    from web_events
    qualify latest_rank = 1
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        latest_rank,
        latest_rank * duration_sec as latest_rank_times_duration_sec
    from ranked
    where event_time > to_date('2024-03-01')
      and page is not null
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(event_time) over (partition by page order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by page order by event_time, id) + interval '30' day then 1 else 0 end as session_start_flag
    from cleaned
),
sessions_web_events as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by page order by event_time, id rows between unbounded preceding and current row) as island_id
    from flagged
),
ranked_web_events as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by page order by latest_rank_times_duration_sec desc, id) as rn
    from sessions_web_events
    qualify rn = 1
)
select
    id,
    event_type,
    page,
    event_time,
    latest_rank,
    session_start_flag,
    island_id,
    rn
from ranked_web_events
order by event_time desc nulls last;
