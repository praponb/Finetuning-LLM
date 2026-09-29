select
    t.id,
    t.merchant,
    t.is_flagged,
    a.account_type,
    c.name,
    c.country
from transactions t
inner join accounts a on t.account_id = a.id
inner join customers c on a.customer_id = c.id
order by account_type, country
