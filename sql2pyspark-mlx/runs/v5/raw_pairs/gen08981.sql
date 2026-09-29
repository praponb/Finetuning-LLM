select
    we.id,
    we.duration_sec,
    we.page,
    s.customer_id,
    c.age,
    c.email
from web_events we
left join sessions s on we.session_id = s.id
inner join customers c on c.id = s.customer_id
where to_date(s.started_at) >= '2024-02-27' and hour(s.started_at) between 12 and 19;
