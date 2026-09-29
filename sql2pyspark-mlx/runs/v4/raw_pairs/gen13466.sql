select
    t.txn_time,
    t.is_flagged,
    a.opened_date,
    c.segment
from transactions t
join accounts a on a.id = t.account_id
left join customers c on c.id = a.customer_id
where c.country in ('TH', 'SG') and c.segment = 'Corporate';
