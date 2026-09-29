select
    s.country,
    s.customer_id,
    c.is_active,
    c.credit_limit,
    c.name
from sessions s
inner join customers c on s.customer_id = c.id
