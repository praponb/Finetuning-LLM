select date_format(txn_time, 'yyyy-MM') as txn_month, sum(case when txn_type = 'REFUND' then amount else 0 end) as refund_amount, min(txn_time) as min_txn_time
from transactions
where amount between 2500 and 3900 and (txn_time < '2024-01-08' or hour(txn_time) between 8 and 16)
group by txn_month
