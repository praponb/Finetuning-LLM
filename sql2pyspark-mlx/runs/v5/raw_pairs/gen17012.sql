select
    s.id,
    s.device,
    s.started_at,
    s.country
from sessions s
where not exists (select 1 from web_events we where we.session_id = s.id and we.event_type <> 'page_view');
