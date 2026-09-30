select
    t.id,
    t.account_id,
    t.amount,
    t.txn_type
from transactions t
where hour(t.txn_time) between 9 and 14 and t.account_id in (select id from accounts where balance >= 38000)
