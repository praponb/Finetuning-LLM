select a.id, a.customer_id, a.balance
from accounts a
where a.id in (select account_id from transactions where txn_type in ('DEBIT', 'CREDIT', 'REFUND'))
