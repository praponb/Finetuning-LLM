select
    a.id,
    a.balance,
    a.customer_id,
    c.segment,
    c.is_active,
    c.country
from accounts a
left join customers c on c.id = a.customer_id
where a.balance <= 61000
order by c.country, customer_id, id
limit 10;
