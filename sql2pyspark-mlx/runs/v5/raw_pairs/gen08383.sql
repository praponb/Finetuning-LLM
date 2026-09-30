select
    a.account_type,
    a.status,
    a.customer_id,
    t.amount,
    t.txn_time
from accounts a
inner join transactions t on a.id = t.account_id
where a.customer_id is not null and t.account_id in (25, 38);
