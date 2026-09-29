with not_flagged as (
    select s.*
    from sessions s
    where exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.event_time >= '2023-05-01'
        )
)
select
    id,
    device,
    browser,
    started_at,
    country
from not_flagged;
