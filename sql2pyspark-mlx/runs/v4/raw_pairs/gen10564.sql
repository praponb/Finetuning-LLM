select
    we.id,
    we.page,
    s.country,
    s.started_at,
    c.age
from web_events we
left outer join sessions s on we.session_id = s.id
inner join customers c on c.id = s.customer_id
