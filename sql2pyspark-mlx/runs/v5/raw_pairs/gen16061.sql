select
    id,
    merchant,
    to_date(txn_time) as txn_time_date,
    cast(is_flagged as int) as is_flagged_flag
from transactions
where is_flagged and amount <= 3100
order by is_flagged_flag desc
