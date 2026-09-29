select *, trim(merchant) clean_merchant
from transactions
where account_id in (4, 8) or not is_flagged
order by txn_time, amount desc
