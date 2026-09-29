select
    t.id,
    t.txn_time,
    t.txn_type,
    a.balance
from transactions t
right join accounts a on t.account_id = a.id
where t.merchant in ('Grab', 'Amazon', 'Shell');
