select
    id,
    account_type,
    opened_date,
    avg(balance) over (order by opened_date, id rows between 6 preceding and current row) as balance_avg_6,
    sum(balance) over (partition by account_type, customer_id order by opened_date, id rows between unbounded preceding and current row) as running_balance,
    sum(balance) over (partition by account_type order by balance, id rows between unbounded preceding and current row) as cumulative_balance,
    cume_dist() over (order by balance nulls last) as balance_cume_dist,
    ntile(10) over (partition by account_type order by balance desc, id) as balance_bucket
from accounts
where balance is not null
