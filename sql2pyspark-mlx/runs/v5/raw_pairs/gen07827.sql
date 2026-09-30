with with_gaps as (
    select *, case when event_time - lag(event_time) over (partition by session_id order by event_time, id) > interval '14' day then 1 else 0 end as session_start_flag
    from web_events
),
sessions_web_events as (
    select *, sum(session_start_flag) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as island_id
    from with_gaps
)
select
    id,
    event_type,
    page,
    duration_sec,
    session_start_flag,
    island_id
from sessions_web_events
order by session_start_flag desc nulls first
