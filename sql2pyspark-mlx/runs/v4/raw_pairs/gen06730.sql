select
    s.customer_id,
    s.started_at,
    we.duration_sec,
    we.event_type,
    we.session_id
from sessions s
left join web_events we on we.session_id = s.id;
