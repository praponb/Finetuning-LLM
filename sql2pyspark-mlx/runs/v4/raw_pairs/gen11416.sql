select
    t.is_flagged,
    t.amount,
    t.account_id,
    a.account_type,
    a.balance
from transactions t
left join accounts a on t.account_id = a.id
where a.opened_date < '2024-08-15'
