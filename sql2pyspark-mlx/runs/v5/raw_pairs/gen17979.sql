select
    id,
    txn_type,
    txn_time,
    amount,
    hour(txn_time) as txn_time_hour
from transactions
order by txn_time_hour desc
