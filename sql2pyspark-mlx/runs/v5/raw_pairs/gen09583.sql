select
    is_flagged,
    txn_type,
    count(amount) as amount_count,
    avg(amount) as avg_amount
from transactions
group by is_flagged, txn_type;
