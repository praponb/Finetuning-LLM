select
    we.id,
    we.duration_sec,
    we.event_type,
    s.browser,
    c.signup_date,
    c.is_active
from web_events we
left join sessions s on we.session_id = s.id
left join customers c on s.customer_id = c.id
