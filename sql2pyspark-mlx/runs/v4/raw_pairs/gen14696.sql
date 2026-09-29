select
    a.id,
    a.account_type,
    a.opened_date,
    a.status,
    c.city,
    c.name,
    c.age
from accounts a
left join customers c on a.customer_id = c.id
where a.balance >= 62000 and a.balance > 43000
order by a.status;
