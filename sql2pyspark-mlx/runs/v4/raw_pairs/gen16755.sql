select
    id,
    txn_type,
    cast(is_flagged as int) as is_flagged_flag,
    floor(amount) as amount_whole
from transactions
where merchant = 'Lazada' and is_flagged = false and txn_time >= '2025-01-14'
order by amount_whole;
