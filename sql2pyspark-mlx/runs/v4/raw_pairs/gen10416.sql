select
    t.is_flagged,
    a.opened_date,
    a.account_type,
    a.customer_id,
    cast(t.is_flagged as int) as is_flagged_flag
from transactions t
inner join accounts a on a.id = t.account_id
where a.account_type <> 'CREDIT'
