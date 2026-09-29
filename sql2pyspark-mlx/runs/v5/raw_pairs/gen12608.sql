select a.id, a.balance, a.account_type
from accounts a
where a.id in (select account_id from transactions)
order by account_type desc
