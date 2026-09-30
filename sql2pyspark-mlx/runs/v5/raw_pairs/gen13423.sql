with flagged as (
    select *, case when lag(event_time) over (partition by page order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by page order by event_time, id) + interval '7' day then 1 else 0 end as new_group_flag
    from web_events
)
select *, sum(new_group_flag) over (partition by page order by event_time, id rows between unbounded preceding and current row) as session_no
from flagged
