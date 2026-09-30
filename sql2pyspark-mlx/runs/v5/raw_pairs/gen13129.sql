with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when event_time - lag(event_time) over (partition by page order by event_time, id) > interval '14' day then 1 else 0 end as new_group_flag
    from web_events
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        session_id,
        page,
        event_time,
        sum(new_group_flag) over (partition by page order by event_time, id rows between unbounded preceding and current row) as burst_id
    from flagged
)
select
    s.id,
    s.session_id,
    s.page,
    s.event_time,
    s.burst_id,
    (select sum(x.duration_sec) from web_events x where x.session_id = s.session_id) as sum_web_event_duration_sec
from islands s
order by page nulls first, burst_id
