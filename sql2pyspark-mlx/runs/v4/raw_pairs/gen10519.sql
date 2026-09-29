select
    s.id,
    s.customer_id,
    c.signup_date,
    c.segment,
    length(s.plan) as plan_length
from subscriptions s
left outer join customers c on s.customer_id = c.id
