select
    merchant,
    account_id,
    is_flagged,
    trim(merchant) as clean_merchant
from transactions
where merchant is null and txn_time < '2025-07-22' and account_id = 36
order by merchant
