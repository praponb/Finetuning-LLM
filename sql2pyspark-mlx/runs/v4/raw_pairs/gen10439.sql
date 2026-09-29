select
    s.end_date,
    s.monthly_fee,
    s.status,
    c.segment,
    c.email
from subscriptions s
right join customers c on s.customer_id = c.id
where not (lower(trim(c.name)) = 'mia rossi' and month(s.start_date) in (1, 2, 10));
