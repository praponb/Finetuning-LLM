with accounts_by_customer as (
    select customer_id, sum(balance) total_balance
    from accounts
    where status = 'CLOSED'
    group by customer_id
)
select
    c.id,
    c.email,
    c.name,
    abc.total_balance
from customers c
join accounts_by_customer abc on c.id = abc.customer_id
where abc.total_balance >= 235000
order by abc.total_balance;
