select id, account_id, trim(txn_type) as clean_txn_type
from transactions
where to_date(txn_time) < '2024-04-01';
