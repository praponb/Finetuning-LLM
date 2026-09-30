with with_gaps as (
    select *, case when event_time - lag(event_time) over (partition by session_id order by event_time, id) > interval 90 days then 1 else 0 end as new_group_flag
    from web_events
)
select *, sum(new_group_flag) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as island_id
from with_gaps
