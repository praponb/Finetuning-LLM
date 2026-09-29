select
    p.account_type,
    e.txn_type,
    count(*) as num_rows,
    min(e.amount) as min_amount,
    grouping_id() as grouping_level
from transactions e
left join accounts p on e.account_id = p.id
group by grouping sets ((p.account_type, e.txn_type), (p.account_type), ())
