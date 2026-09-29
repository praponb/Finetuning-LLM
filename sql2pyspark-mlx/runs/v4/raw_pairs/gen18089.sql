with top_web_events as (
    select *, row_number() over (partition by session_id order by event_time desc, id) as rnk
    from web_events
)
select
    id,
    session_id,
    event_time,
    page
from top_web_events
where rnk <= 3
order by session_id, event_time desc
