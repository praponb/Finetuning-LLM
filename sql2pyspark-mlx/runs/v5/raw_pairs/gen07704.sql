select a.id, a.opened_date
from accounts a
where a.status is null and not exists (select 1 from transactions t where t.account_id = a.id and t.amount >= 1900);
