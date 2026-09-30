with filtered as (
    select *, date_trunc('month', txn_time) as txn_time_period
    from transactions
    where merchant is not null
      and merchant is distinct from 'Lazada'
),
with_dims as (
    select
        s.id,
        s.account_id,
        s.txn_type,
        s.amount,
        s.merchant,
        s.is_flagged,
        b.opened_date,
        b.account_type
    from filtered s
    left join accounts b
        on s.account_id = b.id
),
kept_keys as (
    select merchant, account_id
    from with_dims
    intersect all
    select merchant, account_id
    from transactions
    where is_flagged
),
reconciled as (
    select s.*
    from with_dims s
    inner join kept_keys k on s.merchant = k.merchant and s.account_id = k.account_id
),
joined as (
    select
        a.account_id,
        a.opened_date,
        a.account_type,
        b.balance
    from reconciled a
    left join accounts b
        on a.account_id = b.id
),
pivoted as (
    select *
    from (
        select
            account_id,
            account_type,
            balance
        from joined
    ) src
    pivot (
        max(balance)
        for account_type in ('SAVINGS', 'LOAN')
    )
)
select
    s.account_id,
    s.SAVINGS,
    (select sum(t2.amount) from transactions t2 where t2.account_id = s.account_id) as sum_transaction_amount
from pivoted s
order by account_id desc nulls first, sum_transaction_amount desc
limit 20;
