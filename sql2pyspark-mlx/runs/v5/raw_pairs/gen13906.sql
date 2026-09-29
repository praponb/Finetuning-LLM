select
    is_flagged,
    txn_type,
    max(txn_time) as latest_txn_time,
    count(*) as total_count,
    max(amount) - min(amount) as amount_range
from transactions
where txn_type = 'REFUND' and (is_flagged = true or txn_type = 'REFUND')
group by is_flagged, txn_type
order by latest_txn_time
