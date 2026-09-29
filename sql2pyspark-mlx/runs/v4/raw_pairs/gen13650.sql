select
    account_id,
    max(txn_time) as latest_txn_time,
    min(txn_time) as min_txn_time,
    sum(amount) as total_amount
from transactions
group by account_id
having sum(amount) > 18000;
