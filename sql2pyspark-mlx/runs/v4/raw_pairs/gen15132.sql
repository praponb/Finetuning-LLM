with valid_web_events as (
    select *
    from web_events
    where page in ('/product', '/checkout') and session_id in (8, 16, 24) and page in ('/product', '/search', '/home')
),
summary as (
    select
        case when page in ('/home', '/product') then 'priority' else 'other' end as page_group,
        session_id,
        count(*) as cnt,
        max(event_time) as last_event_time
    from valid_web_events
    group by case when page in ('/home', '/product') then 'priority' else 'other' end, session_id
)
select *
from summary
order by session_id;
