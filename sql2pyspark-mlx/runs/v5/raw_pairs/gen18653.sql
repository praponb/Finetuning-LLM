with with_last as (
    select
        s.*,
        latest.txn_time,
        latest.transaction_txn_type
    from accounts s
    left join lateral (
            select c.txn_time, c.txn_type as transaction_txn_type
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) latest on true
)
select
    s.id,
    s.customer_id,
    s.account_type,
    s.opened_date,
    (select sum(x.monthly_fee) from subscriptions x where x.customer_id = s.customer_id) as sum_subscription_monthly_fee
from with_last s
order by account_type
