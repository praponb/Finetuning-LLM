select
    t.is_flagged,
    a.customer_id,
    a.balance,
    c.city,
    c.segment
from transactions t
join accounts a on t.account_id = a.id
left outer join customers c on a.customer_id = c.id
where a.balance >= 72000 and t.txn_type <> 'CREDIT';
