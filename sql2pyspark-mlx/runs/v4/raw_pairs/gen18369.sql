with latest_match as (
    select
        s.*,
        x.event_time,
        x.duration_sec
    from sessions s
    left join lateral (
            select c.event_time, c.duration_sec
            from web_events c
            where c.session_id = s.id
              and c.event_time <= s.started_at
            order by c.event_time desc, c.id desc
            limit 1
        ) x on true
)
select
    id,
    device,
    browser,
    country,
    event_time,
    duration_sec
from latest_match
order by browser;
