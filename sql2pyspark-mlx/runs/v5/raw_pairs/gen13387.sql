with with_gaps as (
    select *, case when event_time - lag(event_time) over (partition by session_id order by event_time, id) > interval 14 days then 1 else 0 end as is_new_session
    from web_events
),
sessionized as (
    select
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        sum(is_new_session) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as session_id_2
    from with_gaps
)
select
    id,
    session_id,
    event_time,
    session_id_2
from sessionized
order by event_time desc, session_id
