with filtered as (
    select *, length(country) as country_length
    from sessions
    where browser is not null
)
select
    s.id,
    s.device,
    s.browser,
    s.started_at,
    s.country,
    x.event_type
from filtered s
left join lateral (
        select c.event_type
        from web_events c
        where c.session_id = s.id
        order by c.event_time desc, c.id desc
        limit 1
    ) x on true;
