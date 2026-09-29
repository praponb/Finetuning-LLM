with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when event_time - lag(event_time) over (partition by page order by event_time asc, id asc) > interval '60' day then 1 else 0 end as is_new_burst
    from web_events
)
-- Running sum of the flag = session number
select
    id,
    session_id,
    page,
    event_time,
    duration_sec,
    sum(is_new_burst) over (partition by page order by event_time asc, id asc rows between unbounded preceding and current row) as burst_id
from flagged;
