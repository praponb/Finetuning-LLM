select
    we.id,
    we.page,
    we.event_type,
    s.browser,
    s.customer_id,
    s.device
from web_events we
left outer join sessions s on s.id = we.session_id
where s.country not in ('TH', 'US', 'DE');
