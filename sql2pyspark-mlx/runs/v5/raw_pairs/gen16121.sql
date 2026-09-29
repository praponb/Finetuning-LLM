select id, merchant, account_id
from transactions
where txn_type in ('FEE', 'CREDIT', 'DEBIT')
intersect
select id, merchant, account_id
from transactions
where account_id in (6, 38);
