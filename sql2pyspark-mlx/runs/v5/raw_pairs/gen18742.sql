select
    t.id,
    t.merchant,
    t.txn_type,
    t.account_id
from transactions t
where t.account_id not in (select id from accounts where status in ('FROZEN', 'CLOSED'));
