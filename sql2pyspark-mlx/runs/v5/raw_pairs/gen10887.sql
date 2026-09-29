select
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    avg(balance) over (partition by customer_id, account_type) as customer_id_avg_balance,
    cume_dist() over (order by balance) as balance_cume_dist,
    sum(balance) over (order by opened_date, id rows between unbounded preceding and current row) as running_balance,
    sum(balance) over w_running as cumulative_balance,
    lead(balance, 1) over (partition by status order by opened_date, id) as next_balance
from accounts
window w_running as (partition by status, account_type order by opened_date, id rows between unbounded preceding and current row);
