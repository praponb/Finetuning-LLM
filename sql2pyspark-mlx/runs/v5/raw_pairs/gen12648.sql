select
    a.account_type,
    a.status,
    e.merchant,
    count(*) as cnt,
    max(e.amount) as max_amount,
    grouping_id() as level_id
from transactions e
join accounts a on e.account_id = a.id
group by cube (a.account_type, a.status, e.merchant)
