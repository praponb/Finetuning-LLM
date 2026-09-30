select
    c.name,
    c.country,
    s.device,
    s.browser,
    s.started_at
from customers c
join sessions s on s.customer_id = c.id
where c.credit_limit < 14900;
