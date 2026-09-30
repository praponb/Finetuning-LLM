select
    s.id,
    s.device,
    s.browser,
    s.started_at
from sessions s
where not exists (select 1 from web_events we where we.session_id = s.id)
order by s.device
