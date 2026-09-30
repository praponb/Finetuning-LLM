select
    id,
    customer_id,
    account_type,
    status,
    lag(balance) over w_ordered prev_balance,
    row_number() over w_ordered seq_no,
    avg(balance) over (partition by customer_id order by balance, id rows between 5 preceding and current row) balance_avg_5
from accounts
window w_ordered as (partition by account_type order by balance, id);
