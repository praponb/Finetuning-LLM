select
    a.opened_date,
    t.is_flagged,
    t.amount,
    t.account_id
from accounts a
inner join transactions t on a.id = t.account_id
where a.customer_id = 6
