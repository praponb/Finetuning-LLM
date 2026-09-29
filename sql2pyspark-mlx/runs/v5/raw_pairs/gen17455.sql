with valid as (
    select
        *,
        round(balance * 1.1, 2) as balance_adjusted,
        case when balance >= 75000 then 'High' when balance >= 10000 then 'Medium' else 'Low' end as balance_band
    from accounts
    where customer_id in (2, 5, 9, 28)
)
select s.*, top1.transaction_txn_type
from valid s
left join lateral (
        select c.txn_type as transaction_txn_type
        from transactions c
        where c.account_id = s.id
        order by c.txn_time desc, c.id desc
        limit 1
    ) top1 on true
