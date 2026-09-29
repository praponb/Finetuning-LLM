select t.id, t.account_id
from transactions t
where t.account_id not in (select id from accounts where customer_id in (13, 36));
