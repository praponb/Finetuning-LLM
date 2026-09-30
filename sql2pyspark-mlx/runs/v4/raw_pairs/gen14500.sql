select
    id,
    merchant,
    txn_time,
    amount,
    lag(amount, 1) over (partition by merchant order by txn_time, id) as prev_amount,
    min(amount) over (partition by merchant) as merchant_min_amount
from transactions
order by amount, prev_amount desc;
