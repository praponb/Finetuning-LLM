select t.id, t.txn_time, t.amount
from transactions t
where t.account_id not in (select id from accounts where account_type not in ('SAVINGS', 'LOAN', 'CREDIT'));
