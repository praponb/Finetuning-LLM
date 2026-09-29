select quarter(txn_time) as txn_quarter, min(txn_time) as min_txn_time, avg(amount) as avg_amount
from transactions
where txn_time >= '2025-10-06' and (account_id in (10, 14, 15, 30) or amount is null)
group by 1
order by min_txn_time;
