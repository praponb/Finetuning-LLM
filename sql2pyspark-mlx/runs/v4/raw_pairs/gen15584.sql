select
    account_id,
    hour(txn_time) as txn_hour,
    txn_type,
    max(amount) as max_amount
from transactions
group by account_id, hour(txn_time), txn_type
order by account_id desc
