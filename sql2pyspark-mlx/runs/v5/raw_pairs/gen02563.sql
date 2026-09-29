select a.status, t.is_flagged
from accounts a
left outer join transactions t on t.account_id = a.id
where a.customer_id is not null or t.is_flagged = true
order by status
