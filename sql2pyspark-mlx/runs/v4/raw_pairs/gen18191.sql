select
    a.id,
    a.customer_id,
    a.status,
    a.balance,
    c.segment
from accounts a
inner join customers c on c.id = a.customer_id
order by a.customer_id, id;
