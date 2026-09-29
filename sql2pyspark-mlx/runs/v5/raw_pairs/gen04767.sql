select
    we.event_time,
    we.session_id,
    s.started_at,
    s.customer_id,
    c.city,
    c.signup_date
from web_events we
inner join sessions s on we.session_id = s.id
inner join customers c on s.customer_id = c.id
where c.email like '%@startup.io';
