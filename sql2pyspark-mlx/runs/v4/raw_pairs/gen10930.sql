select
    s.started_at,
    s.browser,
    we.session_id,
    we.event_type
from sessions s
join web_events we on s.id = we.session_id
where s.country in ('SG', 'UK', 'JP')
order by event_type, started_at desc;
