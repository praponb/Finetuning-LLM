select a.id, a.status, a.opened_date
from accounts a
where not exists (select 1 from transactions t where t.account_id = a.id and t.txn_time >= '2025-03-01')
