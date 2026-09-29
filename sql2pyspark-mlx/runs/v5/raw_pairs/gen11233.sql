select
    we.event_type,
    we.duration_sec,
    s.browser,
    c.age,
    c.credit_limit,
    trim(we.event_type) as clean_event_type
from web_events we
left join sessions s on s.id = we.session_id
left join customers c on c.id = s.customer_id
order by we.duration_sec
