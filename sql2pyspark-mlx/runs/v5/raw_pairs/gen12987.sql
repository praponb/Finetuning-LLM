with ranked_web_events as (
    select *, row_number() over (partition by session_id order by event_time desc, id) as row_num
    from web_events
)
select *
from ranked_web_events
where row_num <= 1
order by id asc;
