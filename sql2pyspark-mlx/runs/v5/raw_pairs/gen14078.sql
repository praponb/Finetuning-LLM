select round(stddev(amount), 2) stddev_amount, sum(case when txn_type = 'REFUND' then amount else 0 end) refund_amount, count(distinct amount) unique_txn_types
from transactions
order by stddev_amount
