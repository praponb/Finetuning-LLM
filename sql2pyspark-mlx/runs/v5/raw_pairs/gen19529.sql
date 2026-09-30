select t.amount, t.merchant, a.opened_date
from transactions t
join accounts a on t.account_id = a.id
order by t.amount;
