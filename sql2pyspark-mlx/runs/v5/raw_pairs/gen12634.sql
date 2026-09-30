with prev_events as (
    -- Previous event time per key
    select *, lag(event_time) over (partition by event_type order by event_time, id) as prev_event_time
    from web_events
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_event_time is null or event_time > prev_event_time + interval 60 days then 1 else 0 end as new_group_flag
    from prev_events
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as session_id_2
    from gaps
)
select
    id,
    event_type,
    event_time,
    duration_sec,
    prev_event_time,
    new_group_flag
from islands;
