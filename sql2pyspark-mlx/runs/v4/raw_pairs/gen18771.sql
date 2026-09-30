select
    date_trunc('week', txn_time) as week_start,
    txn_type,
    count(*) as row_count,
    percentile_approx(amount, 0.5) as amount_p50,
    count_if(txn_type = 'REFUND') as refund_events,
    count_if(txn_type = 'FEE') as fee_events,
    max(txn_time) as last_seen
from transactions
group by date_trunc('week', txn_time), txn_type
