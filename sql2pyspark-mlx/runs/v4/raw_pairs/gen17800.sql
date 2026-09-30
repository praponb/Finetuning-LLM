select
    a.id,
    a.opened_date,
    a.status,
    a.account_type
from accounts a
where a.id in (select account_id from transactions)
order by a.opened_date, id
limit 10;
