select
    we.id,
    we.page,
    s.device,
    c.signup_date
from web_events we
right join sessions s on s.id = we.session_id
left join customers c on s.customer_id = c.id
where c.segment is null;
