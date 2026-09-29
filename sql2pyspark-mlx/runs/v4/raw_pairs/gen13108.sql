select *, trim(txn_type) as clean_txn_type, case when is_flagged then 'Yes' else 'No' end as is_flagged_label
from transactions
where txn_time < '2024-02-16';
