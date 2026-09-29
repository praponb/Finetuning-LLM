select
    t.id,
    t.txn_type,
    t.is_flagged,
    t.amount
from transactions t
where t.txn_time < '2024-03-07' and t.account_id in (select id from accounts where account_type = 'CREDIT')
