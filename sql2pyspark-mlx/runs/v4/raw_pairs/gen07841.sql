select a.id, a.status, a.opened_date
from accounts a
where a.id in (select account_id from transactions where txn_type not in ('FEE', 'CREDIT', 'REFUND'))
order by status asc;
