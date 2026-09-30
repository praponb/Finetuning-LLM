with ranked as (
    select
        id,
        session_id,
        duration_sec,
        row_number() over (partition by session_id order by duration_sec desc, id) as rnk
    from web_events
)
select *
from ranked
where rnk <= 1
order by session_id desc;
