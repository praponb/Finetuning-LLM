select
    web_events.id,
    web_events.duration_sec,
    sessions.started_at,
    customers.city,
    customers.is_active
from web_events
join sessions on sessions.id = web_events.session_id
left join customers on sessions.customer_id = customers.id
where sessions.country <> 'JP'
