select
    s.id,
    s.customer_id,
    s.balance,
    s.opened_date,
    s.status,
    top1.txn_type
from accounts s
left join lateral (
        select c.txn_type
        from transactions c
        where c.account_id = s.id
        order by c.amount desc, c.id desc
        limit 1
    ) top1 on true;
