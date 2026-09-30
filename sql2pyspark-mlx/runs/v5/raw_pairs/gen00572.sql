select
    s.id,
    s.browser,
    s.customer_id,
    c.credit_limit,
    c.name
from sessions s
left join customers c on c.id = s.customer_id
order by c.credit_limit desc, 1 desc;
