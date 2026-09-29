select
    s.monthly_fee,
    s.customer_id,
    c.age,
    c.segment,
    c.email
from subscriptions s
full outer join customers c on c.id = s.customer_id;
