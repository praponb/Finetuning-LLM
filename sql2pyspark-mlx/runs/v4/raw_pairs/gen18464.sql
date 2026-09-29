with gaps as (
    select *, case when event_time - lag(event_time) over (partition by event_type order by event_time, id) > interval 30 days then 1 else 0 end as is_new_burst
    from web_events
),
islands as (
    select *, sum(is_new_burst) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as session_id_2
    from gaps
)
select
    id,
    page,
    event_time,
    duration_sec,
    is_new_burst
from islands
order by is_new_burst desc nulls first
