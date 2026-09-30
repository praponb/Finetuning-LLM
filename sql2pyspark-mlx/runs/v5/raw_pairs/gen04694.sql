select
    id,
    merchant,
    txn_time,
    amount,
    lead(amount, 1, 0) over (partition by merchant order by txn_time, id) as next_amount
from transactions
