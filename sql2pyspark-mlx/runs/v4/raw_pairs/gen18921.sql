select
    id,
    merchant,
    txn_type,
    txn_time,
    amount,
    sum(amount) over (partition by merchant, txn_type order by txn_time, id) as running_amount,
    max(amount) over (partition by txn_type) as txn_type_max_amount
from transactions;
