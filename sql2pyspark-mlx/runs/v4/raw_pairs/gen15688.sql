select
    s.id,
    s.country,
    s.started_at,
    s.customer_id
from sessions s
where s.browser is not null and not exists (select 1 from web_events we where we.session_id = s.id)
order by s.id desc, 2
