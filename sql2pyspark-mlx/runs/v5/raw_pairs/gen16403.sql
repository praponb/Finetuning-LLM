select
    customer_id,
    status,
    min(balance) min_balance,
    avg(balance) avg_balance
from accounts
where balance between 25000 and 38000
group by 1, 2
order by min_balance desc, avg_balance
