select
    id,
    txn_type,
    txn_time,
    amount,
    lag(amount, 1) over (partition by txn_type order by txn_time, id) as prev_amount,
    count(*) over (partition by txn_type) as txn_type_count
from transactions;
