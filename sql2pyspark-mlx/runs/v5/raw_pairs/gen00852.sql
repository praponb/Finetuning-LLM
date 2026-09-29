select
    t.id,
    t.merchant,
    t.is_flagged,
    a.status,
    a.balance,
    c.credit_limit
from transactions t
full outer join accounts a on t.account_id = a.id
left join customers c on a.customer_id = c.id
