select customer_id, max_balance, min_balance
from (
    select customer_id, max(balance) as max_balance, min(balance) as min_balance
    from accounts
    group by customer_id
) as t
where max_balance >= 55000
order by customer_id desc;
