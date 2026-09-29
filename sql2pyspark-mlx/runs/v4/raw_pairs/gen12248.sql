select is_flagged, txn_time, case when txn_type in ('CREDIT', 'DEBIT') then 'Tier 1' else 'Other' end as txn_type_group
from transactions
order by txn_time, txn_type_group desc;
