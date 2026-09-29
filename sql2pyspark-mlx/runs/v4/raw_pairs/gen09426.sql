select
    t.id,
    t.txn_type,
    t.is_flagged,
    t.merchant,
    a.status,
    round(a.balance, 1) as balance_rounded
from transactions t
full outer join accounts a on a.id = t.account_id
where a.balance >= 62000
order by balance_rounded desc;
