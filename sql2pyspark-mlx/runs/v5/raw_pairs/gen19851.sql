select
    s.status,
    s.plan,
    s.start_date,
    c.city,
    c.credit_limit,
    c.email
from subscriptions s
inner join customers c on s.customer_id = c.id
where s.customer_id is not null;
