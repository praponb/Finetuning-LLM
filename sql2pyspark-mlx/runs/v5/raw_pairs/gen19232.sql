select
    id,
    account_id,
    is_flagged,
    sum(amount) over (partition by txn_type order by amount, id rows between 3 preceding and current row) as last4_sum_amount,
    avg(amount) over (partition by txn_type order by amount, id rows between 3 preceding and current row) as last4_avg_amount,
    count(amount) over (partition by txn_type order by amount, id rows between 3 preceding and current row) as last4_count_amount
from transactions
