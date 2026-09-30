select
    id,
    balance,
    status,
    customer_id,
    first_value(status) over (order by balance desc, id) as first_status,
    percent_rank() over (partition by customer_id, status order by balance) as balance_pct_rank
from accounts
order by status, first_status
