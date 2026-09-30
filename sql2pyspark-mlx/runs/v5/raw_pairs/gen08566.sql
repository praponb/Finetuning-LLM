select c.name, t.unique_statuss, t.avg_balance
from customers c
inner join (select customer_id, count(distinct balance) as unique_statuss, round(avg(balance), 2) as avg_balance
    from accounts
    where status is null
    group by customer_id) t on c.id = t.customer_id
