select
    s.customer_id,
    s.monthly_fee,
    c.is_active,
    c.signup_date
from subscriptions s
full outer join customers c on s.customer_id = c.id
where c.name not like '%Rossi%';
